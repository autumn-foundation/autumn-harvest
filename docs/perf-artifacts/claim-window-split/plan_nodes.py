import re,sys,collections
log=open(sys.argv[1]).read()
entries=re.split(r'\n(?=\S+ \S+ UTC \[\d+\] LOG:  duration:)',log)
agg=collections.defaultdict(list)
for e in entries:
    m=re.search(r'Query Text: (.*)',e)
    if not m: continue
    key=re.sub(r'\s+',' ',m.group(1))[:110]
    plan=e[m.end():]
    nodes=len(re.findall(r'(^\t\s*->|^\t[A-Z][A-Za-z ]+  \(cost)',plan,re.M))
    never=len(re.findall(r'never executed',plan))
    agg[key].append((nodes,never))
rows=[]
for k,v in agg.items():
    n=sum(x for x,_ in v)/len(v); nv=sum(y for _,y in v)/len(v)
    rows.append((len(v)*n,len(v),n,nv,k))
rows.sort(reverse=True)
print("sampled*nodes  samples  nodes/plan  never-exec/plan  statement")
for r in rows[:16]: print(f"{r[0]:10.0f} {r[1]:7d} {r[2]:8.1f} {r[3]:8.1f}  {r[4]}")
