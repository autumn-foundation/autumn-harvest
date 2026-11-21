import re,sys
s=open(sys.argv[1]).read(); mode=sys.argv[2]
def ctes(s):
    # parse top-level WITH list into ordered (name, text) + tail
    assert s.startswith("WITH ")
    i=5; out=[]; n=len(s)
    while True:
        m=re.compile(r'\s*([a-z_]+) AS (MATERIALIZED )?\(').match(s,i)
        if not m: break
        name=m.group(1); j=m.end(); depth=1
        while depth: 
            c=s[j]; depth += (c=='(') - (c==')'); j+=1
        out.append((name,s[m.start():j].strip())); i=j
        m2=re.compile(r'\s*,').match(s,i)
        if m2: i=m2.end()
        else: break
    return out, s[i:]
cl,tail=ctes(s)
names=[n for n,_ in cl]
drop={"concurrency_pending_keys","concurrency_running_counts","seek_deep_rows","seek_deep_window","seek_deep_bounds","seek_deep_pending_keys","seek_deep_running_counts","seek_deep_candidate","legacy_candidate"}
if mode=="nohold": drop|={"seek_held","seek_contended"}
keep=[]
for n,t in cl:
    if n in drop: continue
    if n=="candidate": t="candidate AS ( SELECT * FROM seek_candidate )"
    keep.append(t)
print("WITH "+", ".join(keep)+" "+tail.strip())
print(names,file=sys.stderr)
