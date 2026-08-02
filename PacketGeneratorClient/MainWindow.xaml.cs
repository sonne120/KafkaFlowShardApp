using System.Windows;
using Grpc.Net.Client;
using KafkaFlowShardApp.Ingest.Grpc;

namespace KafkaFlowShardApp.PacketGeneratorClient;

public partial class MainWindow : Window
{
    private readonly PacketFactory _factory = new();

    public MainWindow()
    {
        InitializeComponent();
    }

    private async void SendButton_Click(object sender, RoutedEventArgs e)
    {
        var url = UrlBox.Text.Trim();
        var useSsl = SslCheck.IsChecked == true;
        if (!int.TryParse(CountBox.Text, out var count) || count <= 0)
        {
            Log("Enter a positive packet count.");
            return;
        }

        SendButton.IsEnabled = false;
        StatusText.Text = "Sending…";
        try
        {
            // SSL toggle (false position by default): with TLS off we talk plaintext HTTP/2
            // (h2c) over an http:// address, which gRPC only allows once this switch is set.
            if (!useSsl)
                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            using var channel = GrpcChannel.ForAddress(url);
            var client = new PacketIngest.PacketIngestClient(channel);

            Log($"Connecting to {url} (SSL={(useSsl ? "on" : "off")}), streaming {count} packet(s)…");

            using var call = client.SendStream();
            for (var i = 0; i < count; i++)
            {
                var packet = _factory.Next();
                await call.RequestStream.WriteAsync(packet);
                Log($"→ {packet.Proto,-5} {packet.SourceIp}:{packet.SourcePort} -> {packet.DestIp}:{packet.DestPort}");
            }
            await call.RequestStream.CompleteAsync();

            var reply = await call;
            StatusText.Text = $"Done: {reply.Accepted} accepted";
            Log($"✓ Server accepted {reply.Accepted} packet(s): {reply.Message}");
        }
        catch (Exception ex)
        {
            StatusText.Text = "Error";
            Log($"✗ {ex.Message}");
        }
        finally
        {
            SendButton.IsEnabled = true;
        }
    }

    private void Log(string line)
    {
        LogBox.AppendText(line + Environment.NewLine);
        LogBox.ScrollToEnd();
    }
}
