using System.Security.Claims;
using System.Text;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.IdentityModel.Tokens;
using PacketShard.LoadBalancer;
using PacketShard.ServiceDiscovery;
using Yarp.ReverseProxy.ServiceDiscovery;

// PacketShard API gateway. YARP still does the routing and round-robin load balancing, but the
// gateway now owns the things a bare reverse proxy does not: a single entry point for both
// protocols (gRPC ingress + the srv_read REST API), JWT authentication, per-caller rate
// limiting, TLS termination, and destination health checks.
//
// Auth and TLS are config toggles that default to OFF, so the stack still boots with no
// identity provider and no certificate — the same convention the original SSL switch used.

const string AuthPolicy = "gateway";
const string RateLimitPolicy = "gateway-limit";

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

builder.Services.AddReverseProxy()
    .LoadFromConfig(config.GetSection("ReverseProxy"));

// --- Service discovery -------------------------------------------------------------------
// Cluster destinations are written as "discover://<service>:<port>" rather than as host names.
// The resolver expands each one into the instances behind it and re-runs whenever that set
// changes, so scaling srv_ingest is a deployment concern and not a gateway config edit.
//
// Which registry answers is Discovery:Provider — a Consul agent under Compose, Cloud Map DNS on
// ECS, or the fixed Discovery:Fallback list with neither, which is the replica list the gateway
// used before there was anything to ask. One sentinel, three deployments, no config fork.
builder.Services.AddServiceRegistration(config);
builder.Services.AddSingleton<IDestinationResolver, ServiceDestinationResolver>();

// --- Authentication (off by default) -----------------------------------------------------
// Two ways to validate a bearer token: point Auth:Authority at an OIDC issuer and let the
// handler fetch its signing keys, or set Auth:SigningKey for a symmetric HS256 key, which is
// enough for local development and tests.
var authEnabled = config.GetValue("Auth:Enabled", false);

if (authEnabled)
{
    var authority = config["Auth:Authority"];
    var signingKey = config["Auth:SigningKey"];
    var issuer = config["Auth:Issuer"];
    var audience = config["Auth:Audience"];

    builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
        .AddJwtBearer(options =>
        {
            if (!string.IsNullOrWhiteSpace(authority))
                options.Authority = authority;

            options.RequireHttpsMetadata = config.GetValue("Auth:RequireHttpsMetadata", true);
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuer = !string.IsNullOrWhiteSpace(issuer),
                ValidIssuer = issuer,
                ValidateAudience = !string.IsNullOrWhiteSpace(audience),
                ValidAudience = audience,
                ValidateIssuerSigningKey = true,
                ValidateLifetime = true,
                IssuerSigningKey = string.IsNullOrWhiteSpace(signingKey)
                    ? null
                    : new SymmetricSecurityKey(Encoding.UTF8.GetBytes(signingKey))
            };
        });
}
else
{
    builder.Services.AddAuthentication();
}

// The routes name this policy unconditionally, so it has to resolve either way: it demands a
// token when auth is on and waves everything through when it is off. That keeps the toggle in
// one place instead of forking the route table.
builder.Services.AddAuthorizationBuilder()
    .AddPolicy(AuthPolicy, policy =>
    {
        if (authEnabled)
            policy.RequireAuthenticatedUser();
        else
            policy.RequireAssertion(_ => true);
    });

// --- Rate limiting -----------------------------------------------------------------------
// Fixed window, partitioned per caller: the token subject when authenticated, otherwise the
// remote IP. Note this bounds *calls*, not packets — a client-streaming SendStream spends one
// permit no matter how many packets travel on it.
var rateLimitEnabled = config.GetValue("RateLimit:Enabled", true);
var permitLimit = config.GetValue("RateLimit:PermitLimit", 100);
var windowSeconds = config.GetValue("RateLimit:WindowSeconds", 10);

builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    options.AddPolicy(RateLimitPolicy, httpContext =>
    {
        var user = httpContext.User;
        var partitionKey =
            user.FindFirst(ClaimTypes.NameIdentifier)?.Value
            ?? user.Identity?.Name
            ?? httpContext.Connection.RemoteIpAddress?.ToString()
            ?? "anonymous";

        if (!rateLimitEnabled)
            return RateLimitPartition.GetNoLimiter(partitionKey);

        return RateLimitPartition.GetFixedWindowLimiter(partitionKey, _ => new FixedWindowRateLimiterOptions
        {
            PermitLimit = permitLimit,
            Window = TimeSpan.FromSeconds(windowSeconds),
            QueueLimit = 0,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            AutoReplenishment = true
        });
    });
});

// --- Listeners ---------------------------------------------------------------------------
// Two ports on purpose, and Kestrel forces the issue: on a plaintext endpoint there is no ALPN
// to negotiate with, so one port cannot serve both h2c and HTTP/1.1 — asking for both downgrades
// every connection to HTTP/1.1 and breaks gRPC. So the gRPC port stays HTTP/2-only and the REST
// port carries HTTP/1.1. Both listeners feed the same YARP pipeline, so auth and rate limiting
// apply identically whichever one traffic arrives on.
var grpcPort = int.TryParse(config["Listen:Port"], out var gp) ? gp : 5001;
var httpPort = int.TryParse(config["Listen:HttpPort"], out var hp) ? hp : 5002;

// --- SSL toggle (false position by default) ----------------------------------------------
// Enable by setting Ssl__Enabled=true and providing a PFX; otherwise the listeners serve
// plaintext, and gRPC clients reach the ingress over an "http://" address (h2c).
var sslEnabled = bool.TryParse(config["Ssl:Enabled"], out var s) && s;
var certPath = config["Ssl:CertPath"] ?? "/https/server.pfx";
var certPassword = config["Ssl:CertPassword"] ?? "11111";

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(grpcPort, listen =>
    {
        listen.Protocols = HttpProtocols.Http2;
        if (sslEnabled)
            listen.UseHttps(certPath, certPassword);
    });

    options.ListenAnyIP(httpPort, listen =>
    {
        // Without TLS there is no ALPN, and Kestrel refuses to guess between h2c and HTTP/1.1
        // on one plaintext port — it would log a warning and serve HTTP/1.1 anyway. So ask for
        // HTTP/1.1 only in the clear, and let both versions negotiate once a certificate is on.
        listen.Protocols = sslEnabled ? HttpProtocols.Http1AndHttp2 : HttpProtocols.Http1;
        if (sslEnabled)
            listen.UseHttps(certPath, certPassword);
    });
});

var app = builder.Build();

app.UseAuthentication();
app.UseAuthorization();
app.UseRateLimiter();

// Gateway liveness — deliberately not proxied, and outside the rate limiter so a probe never
// competes with traffic for permits.
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapReverseProxy();

var provider = config["Discovery:Provider"] ?? "Static";

app.Logger.LogInformation(
    "API gateway listening on :{GrpcPort} (gRPC, {Scheme}, HTTP/2) and :{HttpPort} (REST, {Scheme}) — auth {Auth}, rate limit {RateLimit}, discovery via {Discovery}",
    grpcPort, sslEnabled ? "https" : "h2c", httpPort, sslEnabled ? "https" : "http",
    authEnabled ? "on" : "off",
    rateLimitEnabled ? $"{permitLimit}/{windowSeconds}s per caller" : "off",
    provider.ToLowerInvariant());

app.Run();
