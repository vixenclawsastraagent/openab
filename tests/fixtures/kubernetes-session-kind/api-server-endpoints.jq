def require($condition; $message):
  if $condition then . else error($message) end;

def valid_ipv4:
  if type != "string" then
    false
  else
    split(".") as $parts
    | if ($parts | length) != 4 then
        false
      elif all($parts[]; test("^(0|[1-9][0-9]{0,2})$")) then
        all($parts[]; (tonumber) <= 255)
      else
        false
      end
  end;

def is_ready:
  # EndpointSlice consumers treat a missing or null ready condition as ready.
  if .conditions == null then
    true
  elif (.conditions | type) != "object" then
    error("EndpointSlice endpoint.conditions must be an object or null")
  elif .conditions.ready == null then
    true
  elif (.conditions.ready | type) != "boolean" then
    error("EndpointSlice endpoint.conditions.ready must be boolean or null")
  else
    .conditions.ready
  end;

.items
| require(type == "array"; "EndpointSliceList.items must be an array")
| [
    .[]
    | require(type == "object"; "EndpointSlice item must be an object")
    | require(
        (.addressType | type) == "string";
        "EndpointSlice.addressType must be a string"
      )
    | select(.addressType == "IPv4")
    | . as $slice
    | (
        $slice.ports
        | require(type == "array"; "IPv4 EndpointSlice.ports must be an array")
        | .[]
        | require(type == "object"; "EndpointSlice port must be an object")
        | require(
            .name == null or (.name | type) == "string";
            "EndpointSlice port.name must be a string or null"
          )
        | select(.name == "https")
        | if .protocol == null then
            .
          else
            require(
              (.protocol | type) == "string";
              "EndpointSlice port.protocol must be a string or null"
            )
          end
        # EndpointPort protocol defaults to TCP when omitted.
        | select((.protocol // "TCP") == "TCP")
        | .port
        | require(
            type == "number";
            "Kubernetes API https/TCP EndpointSlice port must be a number"
          )
        | require(
            floor == . and . >= 1 and . <= 65535;
            "Kubernetes API https/TCP EndpointSlice port must be an integer from 1 to 65535"
          )
      ) as $port
    | (
        $slice.endpoints
        | require(
            type == "array";
            "IPv4 EndpointSlice.endpoints must be an array"
          )
        | .[]
        | require(type == "object"; "EndpointSlice endpoint must be an object")
        | select(is_ready)
        | .addresses
        | require(
            type == "array" and length > 0;
            "ready IPv4 EndpointSlice addresses must be a non-empty array"
          )
        | .[]
        | require(
            valid_ipv4;
            "ready IPv4 EndpointSlice address must be canonical IPv4"
          )
      ) as $address
    | "\($address):\($port)"
  ]
| unique
| if length == 0 then
    empty
  elif length == 1 then
    "endpoint=\(.[0])"
  else
    error("Kubernetes API EndpointSlice tuples did not agree")
  end
