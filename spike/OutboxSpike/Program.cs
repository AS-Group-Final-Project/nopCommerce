// OutboxSpike — proves ADR-003, ADR-004, ADR-005 (durable queues + transactional outbox + dispatcher)
//
// Three concurrent roles run in parallel:
//   1. Checkout simulator  — writes OrderPlacedMessage rows to the outbox inside a DB transaction
//   2. Outbox dispatcher   — polls every 1 s; publishes Pending rows to RabbitMQ; marks them Sent
//   3. Bridge consumer     — reads from RabbitMQ; deduplicates on OrderGuid; prints "OpenBoxes received"
//
// Run with RabbitMQ up, then `docker compose stop rabbitmq` mid-run to exercise the outage path.

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Dapper;
using Microsoft.Data.Sqlite;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string EXCHANGE      = "verdemart.orders";
const string QUEUE         = "verdemart.orders.openboxes";
const string DLQ           = "verdemart.orders.openboxes.dlq";
const string ROUTING_KEY   = "order.placed";
const string RABBITMQ_HOST = "localhost";
const string DB_PATH       = "outbox_spike.db";

// ── Shared dedup store (simulates bridge's processed_orders table) ──────────
var processedOrders = new ConcurrentDictionary<string, bool>();
int openBoxesRowCount = 0;

// ── SQLite setup (simulates nopCommerce MSSQL OutboxMessage table) ──────────
using var db = new SqliteConnection($"Data Source={DB_PATH}");
db.Open();
db.Execute("""
    CREATE TABLE IF NOT EXISTS OutboxMessage (
        Id        INTEGER PRIMARY KEY AUTOINCREMENT,
        OrderGuid TEXT    NOT NULL,
        Payload   TEXT    NOT NULL,
        Status    TEXT    NOT NULL DEFAULT 'Pending',
        CreatedAt TEXT    NOT NULL,
        SentAt    TEXT
    )
""");

Console.WriteLine("[spike] SQLite ready. OutboxMessage table created.");
Console.WriteLine("[spike] Connecting to RabbitMQ...");

// ── RabbitMQ factory ─────────────────────────────────────────────────────────
var factory = new ConnectionFactory
{
    HostName = RABBITMQ_HOST,
    AutomaticRecoveryEnabled = true,
    NetworkRecoveryInterval = TimeSpan.FromSeconds(2),
};

// ── Helper: try to get a channel, return null if broker is unreachable ───────
IModel? TryGetChannel(IConnection? conn)
{
    try { return conn?.IsOpen == true ? conn.CreateModel() : null; }
    catch { return null; }
}

IConnection? brokerConn = null;
void EnsureConnection()
{
    if (brokerConn?.IsOpen == true) return;
    try
    {
        brokerConn = factory.CreateConnection();
        using var ch = brokerConn.CreateModel();
        // Declare DLQ first so the main queue can reference it
        ch.QueueDeclare(DLQ, durable: true, exclusive: false, autoDelete: false);
        ch.ExchangeDeclare(EXCHANGE, ExchangeType.Direct, durable: true);
        ch.QueueDeclare(
            queue: QUEUE, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                ["x-dead-letter-exchange"] = "",
                ["x-dead-letter-routing-key"] = DLQ,
            });
        ch.QueueBind(QUEUE, EXCHANGE, ROUTING_KEY);
        Console.WriteLine("[broker] Connected and topology declared.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[broker] Unreachable: {ex.Message}");
        brokerConn = null;
    }
}

EnsureConnection();

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// ═══════════════════════════════════════════════════════════════════════════
// ROLE 1 — Checkout simulator
// Places one order every 3 seconds. Writes to outbox inside a transaction.
// Simulates nopCommerce OrderProcessingService + OrderPlacedConsumer.
// ═══════════════════════════════════════════════════════════════════════════
var checkoutTask = Task.Run(async () =>
{
    int orderNumber = 1;
    while (!cts.Token.IsCancellationRequested)
    {
        await Task.Delay(3000, cts.Token).ConfigureAwait(false);

        var guid    = Guid.NewGuid().ToString();
        var payload = JsonSerializer.Serialize(new
        {
            Version    = 1,
            OrderGuid  = guid,
            OrderId    = orderNumber,
            CustomerId = 42,
            TotalAmount= 99.99m,
            PlacedAtUtc= DateTime.UtcNow,
        });

        // Transactional write — order commit + outbox row in one transaction (ADR-004)
        using var tx = db.BeginTransaction();
        db.Execute(
            "INSERT INTO OutboxMessage (OrderGuid, Payload, Status, CreatedAt) VALUES (@g, @p, 'Pending', @t)",
            new { g = guid, p = payload, t = DateTime.UtcNow.ToString("o") }, tx);
        tx.Commit();

        Console.WriteLine($"[checkout] Order #{orderNumber} committed  guid={guid[..8]}…  → outbox Pending");
        orderNumber++;
    }
}, cts.Token);

// ═══════════════════════════════════════════════════════════════════════════
// ROLE 2 — Outbox dispatcher
// Polls every 1 second. Publishes Pending rows to RabbitMQ.
// If broker is down, rows stay Pending — no data loss (ADR-005).
// ═══════════════════════════════════════════════════════════════════════════
var dispatcherTask = Task.Run(async () =>
{
    while (!cts.Token.IsCancellationRequested)
    {
        await Task.Delay(1000, cts.Token).ConfigureAwait(false);

        var pending = db.Query<OutboxRow>(
            "SELECT Id, OrderGuid, Payload FROM OutboxMessage WHERE Status = 'Pending' ORDER BY Id LIMIT 50")
            .ToList();

        if (pending.Count == 0) continue;

        EnsureConnection();
        using var ch = TryGetChannel(brokerConn);

        if (ch is null)
        {
            Console.WriteLine($"[dispatcher] Broker down — {pending.Count} row(s) remain Pending");
            continue;
        }

        ch.ConfirmSelect();
        var props = ch.CreateBasicProperties();
        props.Persistent    = true;   // ADR-003: persistent delivery
        props.ContentType   = "application/json";
        props.MessageId     = Guid.NewGuid().ToString();

        foreach (var row in pending)
        {
            ch.BasicPublish(EXCHANGE, ROUTING_KEY, props, Encoding.UTF8.GetBytes(row.Payload));
            db.Execute(
                "UPDATE OutboxMessage SET Status='Sent', SentAt=@t WHERE Id=@id",
                new { t = DateTime.UtcNow.ToString("o"), id = row.Id });
            Console.WriteLine($"[dispatcher] Published  guid={row.OrderGuid[..8]}…  → Sent");
        }

        ch.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));
    }
}, cts.Token);

// ═══════════════════════════════════════════════════════════════════════════
// ROLE 3 — Bridge consumer
// Reads from RabbitMQ. Deduplicates on OrderGuid (ADR-009).
// Simulates VerdeMart.OpenBoxesBridge creating a fulfillment order.
// ═══════════════════════════════════════════════════════════════════════════
var consumerTask = Task.Run(async () =>
{
    // Wait for broker to be ready before starting consumer
    while (!cts.Token.IsCancellationRequested)
    {
        EnsureConnection();
        if (brokerConn?.IsOpen == true) break;
        await Task.Delay(2000, cts.Token).ConfigureAwait(false);
    }

    using var ch = brokerConn!.CreateModel();
    ch.BasicQos(0, prefetchCount: 1, global: false);

    var consumer = new EventingBasicConsumer(ch);
    consumer.Received += (_, ea) =>
    {
        var json  = Encoding.UTF8.GetString(ea.Body.ToArray());
        var doc   = JsonSerializer.Deserialize<JsonElement>(json);
        var guid  = doc.GetProperty("OrderGuid").GetString()!;
        var ordId = doc.GetProperty("OrderId").GetInt32();

        if (processedOrders.TryAdd(guid, true))
        {
            var count = Interlocked.Increment(ref openBoxesRowCount);
            Console.WriteLine($"[bridge]      OpenBoxes row created  order=#{ordId}  guid={guid[..8]}…  total={count}");
            ch.BasicAck(ea.DeliveryTag, multiple: false);
        }
        else
        {
            // Duplicate delivery — idempotent discard
            Console.WriteLine($"[bridge]      DUPLICATE discarded    guid={guid[..8]}…  (dedup hit)");
            ch.BasicAck(ea.DeliveryTag, multiple: false);
        }
    };

    ch.BasicConsume(QUEUE, autoAck: false, consumer);
    Console.WriteLine("[bridge]  Consumer started.");

    await Task.Delay(Timeout.Infinite, cts.Token).ConfigureAwait(false);
}, cts.Token);

// ── Status printer ────────────────────────────────────────────────────────
var statusTask = Task.Run(async () =>
{
    while (!cts.Token.IsCancellationRequested)
    {
        await Task.Delay(10000, cts.Token).ConfigureAwait(false);
        var pending = db.ExecuteScalar<int>("SELECT COUNT(*) FROM OutboxMessage WHERE Status='Pending'");
        var sent    = db.ExecuteScalar<int>("SELECT COUNT(*) FROM OutboxMessage WHERE Status='Sent'");
        var dlqDepth = 0;
        try
        {
            using var ch = TryGetChannel(brokerConn);
            if (ch is not null)
            {
                var q = ch.QueueDeclarePassive(DLQ);
                dlqDepth = (int)q.MessageCount;
            }
        }
        catch { /* broker may be down */ }

        Console.WriteLine($"[status]  outbox Pending={pending} Sent={sent} | OpenBoxes rows={openBoxesRowCount} | DLQ depth={dlqDepth}");
    }
}, cts.Token);

Console.WriteLine();
Console.WriteLine("══════════════════════════════════════════════════════");
Console.WriteLine("  Spike running. Orders placed every 3 s.");
Console.WriteLine("  To exercise the outage path:");
Console.WriteLine("    docker compose stop rabbitmq   ← stop broker");
Console.WriteLine("    (watch Pending rows accumulate, no orders lost)");
Console.WriteLine("    docker compose start rabbitmq  ← restart broker");
Console.WriteLine("    (watch dispatcher drain, bridge consume all rows)");
Console.WriteLine("  Press Ctrl+C to stop.");
Console.WriteLine("══════════════════════════════════════════════════════");
Console.WriteLine();

try { await Task.WhenAll(checkoutTask, dispatcherTask, consumerTask, statusTask); }
catch (OperationCanceledException) { /* clean shutdown */ }

Console.WriteLine();
var finalPending = db.ExecuteScalar<int>("SELECT COUNT(*) FROM OutboxMessage WHERE Status='Pending'");
var finalSent    = db.ExecuteScalar<int>("SELECT COUNT(*) FROM OutboxMessage WHERE Status='Sent'");
Console.WriteLine($"[spike] Final state — outbox Pending={finalPending} Sent={finalSent} | OpenBoxes rows={openBoxesRowCount}");

record OutboxRow(long Id, string OrderGuid, string Payload);
