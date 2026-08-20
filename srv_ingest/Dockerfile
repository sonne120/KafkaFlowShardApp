# Build from the PacketShard/ directory: docker build -f srv_ingest/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Shared/Shared.csproj Shared/
COPY kafka/kafka.csproj kafka/
COPY outbox/outbox.csproj outbox/
COPY consul/consul.csproj consul/
COPY srv_ingest/srv_ingest.csproj srv_ingest/
COPY protos/ protos/
RUN dotnet restore srv_ingest/srv_ingest.csproj
COPY Shared/ Shared/
COPY kafka/ kafka/
COPY outbox/ outbox/
COPY consul/ consul/
COPY srv_ingest/ srv_ingest/
RUN dotnet publish srv_ingest/srv_ingest.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=
EXPOSE 8080 8081
ENTRYPOINT ["dotnet", "srv_ingest.dll"]
