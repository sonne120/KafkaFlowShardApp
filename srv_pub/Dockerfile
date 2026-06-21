# Build from the KafkaFlowShardApp/ directory: docker build -f srv_pub/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Shared/Shared.csproj Shared/
COPY kafka/kafka.csproj kafka/
COPY outbox/outbox.csproj outbox/
COPY srv_pub/srv_pub.csproj srv_pub/
RUN dotnet restore srv_pub/srv_pub.csproj
COPY Shared/ Shared/
COPY kafka/ kafka/
COPY outbox/ outbox/
COPY srv_pub/ srv_pub/
RUN dotnet publish srv_pub/srv_pub.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "srv_pub.dll"]
