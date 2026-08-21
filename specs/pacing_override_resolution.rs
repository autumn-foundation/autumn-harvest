use vstd::prelude::*;

verus! {
    pub struct Policy { pub refill: int, pub burst: int }
    pub struct Override { pub refill: Option<int>, pub burst: Option<int>, pub expires_at: int }

    pub open spec fn valid(policy: Policy) -> bool {
        policy.refill > 0 && policy.burst > 0
    }

    pub open spec fn resolve(base: Policy, value: Option<Override>, now: int) -> Policy {
        match value {
            Some(v) if now < v.expires_at => Policy {
                refill: v.refill.unwrap_or(base.refill),
                burst: v.burst.unwrap_or(base.burst),
            },
            _ => base,
        }
    }

    proof fn expired_is_baseline(base: Policy, value: Override, now: int)
        requires now >= value.expires_at
        ensures resolve(base, Some(value), now) == base
    {}

    proof fn partial_inherits(base: Policy, value: Override, now: int)
        requires now < value.expires_at, value.refill.is_none(), value.burst.is_none()
        ensures resolve(base, Some(value), now) == base
    {}

    proof fn positivity_is_preserved(base: Policy, value: Override, now: int)
        requires valid(base),
            value.refill.is_some() ==> value.refill.unwrap() > 0,
            value.burst.is_some() ==> value.burst.unwrap() > 0
        ensures valid(resolve(base, Some(value), now))
    {}
}

fn main() {}
