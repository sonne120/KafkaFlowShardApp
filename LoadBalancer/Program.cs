using Microsoft.AspNetCore.Server.Kestrel.Core;

// YARP reverse proxy that round-robins gRPC (HTTP/2) calls across the srv_ingest replicas.
// Same shape as the original project's LoadBalancer, but TLS is a config toggle instead of
// being hardcoded on — it defaults to OFF (plaintext h2c) so the stack runs without a cert.
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddReverseProxy()
    .LoadFromConfig(builder.Configuration.GetSection("ReverseProxy"));

var port = int.TryParse(builder.Configuration["Listen:Port"], out var p) ? p : 5001;

// --- SSL toggle (false position by default) ---------------------------------------------
// Enable by setting Ssl__Enabled=true and providing a PFX; otherwise the listener serves
// plaintext HTTP/2 (h2c), which gRPC clients reach over an "http://" address.
var sslEnabled = bool.TryParse(builder.Configuration["Ssl:Enabled"], out var s) && s;
var certPath = builder.Configuration["Ssl:CertPath"] ?? "/https/server.pfx";
var certPassword = builder.Configuration["Ssl:CertPassword"] ?? "11111";

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(port, listen =>
    {
        listen.Protocols = HttpProtocols.Http2;
        if (sslEnabled)
            listen.UseHttps(certPath, certPassword);
    });
});

var app = builder.Build();

app.Logger.LogInformation(
    "LoadBalancer listening on :{Port} ({Scheme}, HTTP/2)", port, sslEnabled ? "https" : "h2c");

app.MapReverseProxy();

app.Run();
