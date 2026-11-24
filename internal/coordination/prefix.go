package coordination

import "context"

// Seek past each complete child subtree. The script bounds work per Redis
// call, even when a prefix contains many immediate (non-directory) keys.
const prefixesScript = `
local lower=ARGV[2];local upper=ARGV[3];local prefix=ARGV[4];local delimiter=ARGV[5]
local out={''}
for step=1,256 do
 local ks=redis.call('ZRANGEBYLEX',KEYS[4],lower,upper,'LIMIT',0,1)
 if #ks==0 then return out end
 local k=ks[1];local rest=string.sub(k,#prefix+1)
 local pos=string.find(rest,delimiter,1,true)
 if pos then
  local child=prefix..string.sub(rest,1,pos+#delimiter-1)
  table.insert(out,child)
  local last=#child
  while last>0 and string.byte(child,last)==255 do last=last-1 end
  if last==0 then return out end
  lower='['..string.sub(child,1,last-1)..string.char(string.byte(child,last)+1)
 else lower='('..k end
end
out[1]=lower
return out`

func (s *Store) commonPrefixes(ctx context.Context, prefix, delimiter string) ([]string, error) {
	lower := "[" + prefix
	var out []string
	for {
		v, err := s.eval(ctx, prefixesScript, lower, "["+prefix+"\xff", prefix, delimiter)
		if err != nil {
			return nil, err
		}
		parts := v.([]any)
		for _, p := range parts[1:] {
			out = append(out, p.(string))
		}
		lower = parts[0].(string)
		if lower == "" {
			return out, nil
		}
	}
}
