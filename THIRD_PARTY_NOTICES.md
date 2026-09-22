# Third-Party Notices

The CellHive production image packages these native runtime tools:

| Component | Packaged version | Source artifact | License |
|---|---:|---|---|
| Cloudflare workerd (`@cloudflare/workerd-linux-64`) | `1.20260916.1` | `https://registry.npmjs.org/@cloudflare/workerd-linux-64/-/workerd-linux-64-1.20260916.1.tgz` | Apache-2.0 |
| esbuild (`@esbuild/linux-x64`) | `0.28.2` | `https://registry.npmjs.org/@esbuild/linux-x64/-/linux-x64-0.28.2.tgz` | MIT |

The Docker build verifies the registry-published SHA-512 digest before extracting either binary. License texts are copied into the image at:

- `/usr/share/licenses/cellhive/workerd/LICENSE`
- `/usr/share/licenses/cellhive/esbuild/LICENSE`

Repository copies are in `third_party/workerd/LICENSE` and `third_party/esbuild/LICENSE`.

## Design Reference Not Shipped

CellHive's stock-workerd runtime design references methods described by WDL
(`wdl-dev/wdl`, Apache-2.0), including fixed commit
`dc70da6cc04acee7d31d80fc0caf8f323bbacf21`. CellHive does not copy,
substantially adapt, package, link, or ship WDL source code or build artifacts.
The corresponding CellHive compatibility generator and code/env budget
implementations are independent. This attribution records the design-method
reference and does not add WDL as a runtime or distribution dependency.
