# Build from the KafkaApp/ directory: docker build -f MasterNode/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Shared/Shared.csproj Shared/
COPY MasterNode/MasterNode.csproj MasterNode/
RUN dotnet restore MasterNode/MasterNode.csproj
COPY Shared/ Shared/
COPY MasterNode/ MasterNode/
RUN dotnet publish MasterNode/MasterNode.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
EXPOSE 8000
ENTRYPOINT ["dotnet", "MasterNode.dll"]
