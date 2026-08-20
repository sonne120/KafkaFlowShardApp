# Build from the PacketShard/ directory: docker build -f LoadBalancer/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY discovery/discovery.csproj discovery/
COPY LoadBalancer/LoadBalancer.csproj LoadBalancer/
RUN dotnet restore LoadBalancer/LoadBalancer.csproj
COPY discovery/ discovery/
COPY LoadBalancer/ LoadBalancer/
RUN dotnet publish LoadBalancer/LoadBalancer.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=
EXPOSE 5001 5002
ENTRYPOINT ["dotnet", "LoadBalancer.dll"]
