# Build from the PacketShard/ directory: docker build -f srv_read/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Shared/Shared.csproj Shared/
COPY srv_read/srv_read.csproj srv_read/
RUN dotnet restore srv_read/srv_read.csproj
COPY Shared/ Shared/
COPY srv_read/ srv_read/
RUN dotnet publish srv_read/srv_read.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "srv_read.dll"]
