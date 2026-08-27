# DraNet Fork Architecture — Deep Dive & Isolation Analysis

## 1. REPO DESIGN — TWO DRIVERS IN ONE DAEMON

The daemon (`cmd/dranet/app.go`) registers **two separate kubelet DRA plugins** + **one NRI plugin**, all on the same `NetworkDriver` struct:

```
driverName    = "dra.net"                 ← upstream inventory (PCI/netlink/RDMA discovery)
cnsDriverName = "networking.azure.com"    ← your fork's CNS NIC resources
```

Both call `kubeletplugin.Start(ctx, plugin, ...)` with the same `*NetworkDriver` handler, but different driver names. The kubelet routes claims by driver name, so `networking.azure.com` claims reach `cnsPlugin` and `dra.net` claims reach `draPlugin`. Both end up calling `PrepareResourceClaims()` on the same struct — the `isCNSClaim()` check disambiguates internally.

### Binaries
- **`cmd/dranet/app.go`** → Main daemon. Registers two DRA kubelet plugins + one NRI plugin.
- **`cmd/dranetctl/`** → CLI utility (not relevant to isolation).

### Core Packages
| Package | Role |
|---------|------|
| `pkg/driver/` | All runtime logic: DRA hooks, NRI hooks, SwiftV2 plumbing, resource publishing |
| `pkg/inventory/` | Host NIC/PCI/RDMA discovery → produces `[]resourceapi.Device` |
| `pkg/filter/` | CEL-based device filtering before publishing |
| `pkg/cnsclient/` | Azure CNS REST API client (NIC discovery, goal state) |
| `pkg/apis/` | Types, attributes, config validation |
| `pkg/cloudprovider/` | Cloud instance abstraction (GCP, Azure) |
| `internal/nlwrap/` | Netlink wrapper with EINTR retry |

---

## 2. THE THREE HOOK LAYERS

### Layer 1: Inventory Discovery (`pkg/inventory/db.go`)
- `DB.Run()` → netlink subscription + periodic polling
- `scan()` → PCI → netlink → RDMA → cloud attributes
- Filters out: loopback, `cilium_*`, `docker0`, default gateway interfaces
- Sends `[]resourceapi.Device` on a channel

### Layer 2: DRA Hooks (`pkg/driver/dra_hooks.go`)

**Publishing (outbound):**
- `PublishResources()` — inventory channel → CEL filter → `draPlugin.PublishResources()` → ResourceSlices under `dra.net`
- `PublishCNSResources()` — polls CNS every 5s → builds `networking.azure.com/*` attributed devices → `cnsPlugin.PublishResources()` → ResourceSlices under `networking.azure.com`

**Claim Preparation (inbound from kubelet):**
```go
// dra_hooks.go:354-371
for _, claim := range claims {
    if isCNSClaim(claim) {
        prepareCNSResourceClaim(ctx, claim)  // fast path → populates SwiftV2PodConfigStore
    } else {
        prepareResourceClaim(ctx, claim)     // standard path → populates PodConfigStore
    }
}
```
Both paths also have inner guards: `if result.Driver != np.driverName { continue }` (line 555) and `if result.Driver != np.cnsDriverName { continue }` (line 454).

### Layer 3: NRI Hooks (`pkg/driver/nri_hooks.go` + `swiftv2_nri.go`)

NRI fires for **ALL pods on the node**. Self-filtering happens via the config stores:

```go
// nri_hooks.go:119-126  RunPodSandbox
podConfig, hasDRAConfig := np.podConfigStore.GetPodConfigs(podUID)
swiftV2Configs := np.swiftV2Store.Get(podUID)
if !hasDRAConfig && swiftV2Configs == nil {
    return nil  // ← not our pod, no-op
}
```

- **Upstream path** (`runPodSandbox`): moves NIC to pod netns, configures routes/rules/neighbors/ethtool/eBPF/RDMA, updates ResourceClaim status
- **SwiftV2 path** (`runPodSandboxSwiftV2`): shared ipvlan L3 or dedicated NIC plumbing via CNS goal state
- `CreateContainer` only injects RDMA char devices
- `StopPodSandbox` reverses both paths
- `RemovePodSandbox` removes netns tracking

---

## 3. EXTENSIBILITY SEAMS

| Seam | Where | What it gates |
|------|-------|--------------|
| **CEL filter** | `--filter` flag → `filter.FilterDevices()` in `PublishResources()` | Which inventory devices get published as ResourceSlices |
| **Driver name routing** | `result.Driver != np.driverName` checks in DRA hooks | Which claim devices get processed |
| **PodConfigStore gating** | NRI hooks check both stores before acting | Which pods get NIC plumbing |
| **Option pattern** | `WithFilter()`, `WithInventory()`, `WithCNSClient()`, `WithCNSDriverName()` | Driver construction-time composition |
| **Hookable vars** | `nsAttachSwiftV2NICHook`, `nicExistsInNetnsHook` in `swiftv2_nri.go` | Test seams for NIC operations |
| **Separate kubelet plugins** | `draPlugin` vs `cnsPlugin` | Independent ResourceSlice publishing |

---

## 4. HOW ISOLATED IS YOUR FORK TODAY?

### ✅ Already done:
1. **Upstream publishing disabled** — `go plugin.PublishResources(ctx)` is **commented out** at `driver.go:236-237`. No `dra.net` ResourceSlices are created, so no claims can reference them.
2. **CNS has its own kubelet plugin** — publishes under `networking.azure.com` independently.
3. **`isCNSClaim()` routing** — CNS claims take the fast path, standard claims take the old path.
4. **NRI auto-filters** — no config in stores = no-op for non-CNS pods.

### ⚠️ Gaps / Things to consider:

**Gap 1: The `dra.net` kubelet plugin still registers.** It's needed because the same `*NetworkDriver` handles both plugins. But it means the kubelet sees a `dra.net` driver on the node. If someone creates a `dra.net` ResourceClaim manually (or upstream dranet also runs), the standard `prepareResourceClaim()` path would fire. **Fix:** Add a hard no-op for `!isCNS` in `prepareResourceClaims()`.

**Gap 2: Inventory goroutine still runs** (`driver.go:219-233`). `netdb.Run()` scans PCI/netlink/RDMA every poll interval. The CNS path uses `findLinkByMAC()` directly and doesn't need it. But it's harmless — just wasted CPU cycles. The standard `prepareResourceClaim()` path does need `GetNetInterfaceName()` from inventory, so if you hard-disable the standard path, you could also stop the inventory goroutine.

**Gap 3: `UnprepareResourceClaims` only cleans `podConfigStore`** — it doesn't clean `swiftV2Store`. SwiftV2 cleanup happens in `StopPodSandbox` NRI hook instead. Minor leak risk if unprepare fires without a corresponding stop.

---

## 5. DO YOU NEED CHANGES BEYOND HOOKS?

**Short answer: The hooks are the primary control surface and they're mostly sufficient. But there are 2-3 non-hook changes worth considering:**

| Change | Category | Why |
|--------|----------|-----|
| Hard no-op for non-CNS claims in `prepareResourceClaims()` | DRA hooks | Defense-in-depth: reject `dra.net` claims even if manually created |
| Optionally skip `dra.net` kubelet plugin registration | `driver.go` startup | Cleanest isolation — but requires refactoring since NRI/DRA share the struct |
| Optionally skip inventory goroutine | `driver.go` startup | Eliminate unnecessary scanning if CNS path doesn't need it |
| Add `swiftV2Store` cleanup in `UnprepareResourceClaims` | DRA hooks | Prevent potential config leak |

Everything else — NRI filtering, resource publishing, claim routing — is already gated by the existing hook architecture. The design is well-layered for your use case.

---

## 6. DATA FLOW DIAGRAM

```
                    ┌──────────────────────────────────────────────────┐
                    │              cmd/dranet/app.go                    │
                    │  driverName="dra.net"  cnsDriverName="networking.azure.com" │
                    └────────────┬─────────────────────┬───────────────┘
                                 │                     │
                    ┌────────────▼──────┐   ┌──────────▼────────────┐
                    │  draPlugin        │   │  cnsPlugin             │
                    │  (kubeletplugin)  │   │  (kubeletplugin)       │
                    │  driver: dra.net  │   │  driver: networking.   │
                    │  DISABLED publish  │   │  azure.com             │
                    └────────────┬──────┘   └──────────┬────────────┘
                                 │                     │
                    ┌────────────▼─────────────────────▼────────────┐
                    │           NetworkDriver (shared struct)        │
                    │                                               │
                    │  PrepareResourceClaims()                      │
                    │    ├─ isCNSClaim? → prepareCNSResourceClaim() │
                    │    │                → SwiftV2PodConfigStore    │
                    │    └─ else       → prepareResourceClaim()     │
                    │                  → PodConfigStore              │
                    │                                               │
                    │  NRI Hooks (global, self-filtering):          │
                    │    RunPodSandbox()                            │
                    │      ├─ podConfigStore has config? → upstream │
                    │      ├─ swiftV2Store has config?   → swiftv2  │
                    │      └─ neither?                  → no-op     │
                    └───────────────────────────────────────────────┘
                                 │
                    ┌────────────▼──────────────────┐
                    │  pkg/inventory/db.go          │
                    │  (still runs, but publishing   │
                    │   is commented out)            │
                    └───────────────────────────────┘

    CNS API (Azure) ──────► pkg/cnsclient/ ──────► PublishCNSResources()
                                                      │
                                                      ▼
                                              ResourceSlices under
                                            "networking.azure.com"
```

---

## 7. ASSUMPTIONS

| # | Assumption | Confidence |
|---|-----------|------------|
| A1 | "DRI hooks" = "DRA hooks" (no "DRI" concept in codebase) | HIGH |
| A2 | Kubelet routes claims by driver name; both plugins share the same handler struct and disambiguate via `isCNSClaim()` | HIGH — verified in code |
| A3 | Commenting out `go plugin.PublishResources(ctx)` is sufficient — no other code path triggers inventory publishing | HIGH — grep confirms |
| A4 | Inventory goroutine is not needed for pure CNS path (`findLinkByMAC()` is standalone) | MEDIUM — need to verify no CNS code path calls `GetNetInterfaceName()` |
| A5 | NRI hooks are globally fired (no driver-name filtering at NRI level); self-filtering via config stores is the only gate | HIGH — NRI framework design |
| A6 | This fork runs as a **replacement** for upstream dranet, not alongside it | MEDIUM — if running alongside, the `dra.net` registration would conflict |
| A7 | The "special NIC" you want to publish is the CNS NIC under `networking.azure.com`, not a third driver name | MEDIUM — could be wrong if you're adding a new resource type |

---

## 8. KEY FILE REFERENCE

| File | Purpose |
|------|---------|
| `cmd/dranet/app.go` | Entry point, driver names, flag parsing, driver startup |
| `pkg/driver/driver.go` | `NetworkDriver` struct, `Start()`, kubelet + NRI plugin registration |
| `pkg/driver/dra_hooks.go` | `PublishResources`, `PublishCNSResources`, `PrepareResourceClaims`, `UnprepareResourceClaims` |
| `pkg/driver/nri_hooks.go` | `Synchronize`, `CreateContainer`, `RunPodSandbox`, `StopPodSandbox`, `RemovePodSandbox` |
| `pkg/driver/swiftv2_nri.go` | `runPodSandboxSwiftV2`, `stopPodSandboxSwiftV2` |
| `pkg/driver/swiftv2_network.go` | SwiftV2 NIC attach/detach low-level operations |
| `pkg/driver/swiftv2_store.go` | `SwiftV2PodConfigStore` — pod config for NRI lookups |
| `pkg/driver/claim_resolution.go` | CNS goal state resolution, MAC matching, SwiftV2 config building |
| `pkg/driver/pod_device_config.go` | `PodConfigStore` — upstream DRA pod config |
| `pkg/inventory/db.go` | Host device discovery (PCI, netlink, RDMA, cloud) |
| `pkg/filter/filter.go` | CEL-based device filtering |
| `pkg/cnsclient/cnsclient.go` | Azure CNS REST API client |
| `pkg/apis/attributes.go` | Device attribute constants (`dra.net/*`) |
| `pkg/apis/types.go` | `NetworkConfig`, `InterfaceConfig`, `RouteConfig`, etc. |
