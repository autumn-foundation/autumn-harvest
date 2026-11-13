# Usage: census_diff.py <base-census.txt> <after-census.txt>
# Prints the totals and each statement whose calls or plans moved by more than 20.
import sys,re
def load(p):
    rows={}; totals=None
    for line in open(p):
        parts=line.rstrip('\n').split(' | ')
        if len(parts)>=6 and parts[0].strip().isdigit():
            q=re.sub(r'\s+',' ',parts[5])[:140]
            c,pl,pt,et=int(parts[0]),int(parts[1]),float(parts[2]),float(parts[3])
            r=rows.setdefault(q,[0,0,0.0,0.0]); r[0]+=c; r[1]+=pl; r[2]+=pt; r[3]+=et
        elif len(parts)==4 and all(x.strip().replace('.','').isdigit() for x in parts):
            totals=parts
    return rows,totals
a,ta=load(sys.argv[1]); b,tb=load(sys.argv[2])
print("totals base",ta,"new",tb)
keys=set(a)|set(b)
diff=[]
for k in keys:
    x=a.get(k,[0,0,0,0]); y=b.get(k,[0,0,0,0])
    if abs(x[0]-y[0])>20 or abs(x[1]-y[1])>20:
        diff.append((y[0]-x[0],x,y,k))
for d,x,y,k in sorted(diff):
    print(f"{x[0]:6d}->{y[0]:6d} calls  {x[1]:5d}->{y[1]:5d} plans | {k}")
