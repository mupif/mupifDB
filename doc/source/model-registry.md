# Model registry

mupifDB is the registry of **model packages** run by mupif **execution servers**. An execution server is a generic compute node: it runs any published model on demand, and each package brings its own environment, so nothing has to be installed on the server host for a new model. See the mupif documentation (section *Execution servers and model packages*, modules `mupif.executionserver` and `mupif.modelpackage`) for packages and servers; this page describes the database side.

## Records and files

A published package is a record of the collection `Models` (`mupifDB.models.ModelPackage_Model`), with its archive in GridFS:

| Field | Meaning |
|---|---|
| `ID`, `Version` | model ID (what workflows give as `ModelID`) and package version; unique together |
| `Entrypoint` | the model class, `module:Class` |
| `Runtime` | `Type` (`pixi`: the package's pixi environment; `apptainer`: a container image; `python`: the server's own environment), `Environment` (pixi environment name); for `apptainer`: `Image` (`docker://`/`oras://` pinned by `@sha256:`, or an `https://`/`file:///` SIF file), `ImageDigest` (sha256 of the SIF file), `Gpu`, `Python` |
| `RequiresHost` | host capabilities the model needs (e.g. a licensed solver), configured on execution servers |
| `Description` | free text |
| `Digest` | `sha256:<hex>` of the archive; execution servers verify every download against it |
| `GridFSID`, `FileName`, `Size` | the archive in GridFS, downloaded by `GET /file/{GridFSID}` |
| `Publisher`, `PublishedDate` | who published it, and when |

The manifest fields are read from `mupif-model.toml` of the archive. **Versions are immutable**: publishing an `ID` and `Version` again is refused (also enforced by a unique index), so a version always means the same content.

## REST API

| Request | |
|---|---|
| `POST /models` | publish: multipart field `package`, the zip archive (`python -m mupif.modelpackage publish <dir>` makes and sends it). **Administrators only.** 400: invalid archive (no or invalid manifest, unsafe paths or links, version `latest`); 409: version published already; 413: larger than `MUPIFDB_MAX_MODEL_PACKAGE_BYTES` (default 256 MiB). |
| `GET /models[?id=<ID>]` | records, sorted by ID and version (latest last) |
| `GET /models/{ID}` | all versions of a model; 404 if none |
| `GET /models/{ID}/versions/{Version}` | one version; `latest` for the latest one (by version order: `1.10` after `1.9`). Execution servers resolve model IDs here. |

The archive is checked as execution servers will unpack it; no code of it is imported by the API (unlike workflow uploads). Container images are not stored in mupifDB: the package references them (registry or file), pinned by digest, and execution servers verify them. Execution servers need an ordinary account (reading); its credentials go to `API_CREDENTIALS_FILE` on the server host.

Publishing gives the right to run code on every execution server, which is why it is reserved to administrators.

## Executions

When the workflow of an execution runs models on execution servers, the execution script stores which packages exactly in `ModelPackages` of the execution (model name → `ID`, `Version`, `Digest`), next to `ModelServers` (model name → server). Both are cleared when the execution runs again. The digest identifies the package content, including the locked environment: the provenance of the results.

Workflow records keep `ModelVersion` of their models (`Workflow_Model.Models`): a workflow may pin the package version its model runs; without it, the latest version is used.
