-- Distribute requests across DO_RPS_SHARDS distinct Durable Object ids.
local shards = tonumber(os.getenv("DO_RPS_SHARDS") or "1")
request = function()
  local shard = math.random(shards)
  return wrk.format("GET", "/inc?shard=" .. shard)
end
