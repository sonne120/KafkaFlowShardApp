# Build from the KafkaApp/ directory: docker build -f srv_sub/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Shared/Shared.csproj Shared/
COPY srv_sub/srv_sub.csproj srv_sub/
RUN dotnet restore srv_sub/srv_sub.csproj
COPY Shared/ Shared/
COPY srv_sub/ srv_sub/
RUN dotnet publish srv_sub/srv_sub.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "srv_sub.dll"]
