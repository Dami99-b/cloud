# Distributed Cloud File Storage Engine

A file storage backend that behaves the way you'd want one to behave in production: bytes go straight from the browser to object storage, the API only ever handles the handshake, and everything expensive happens later on a worker.

Upload a 4 GB video and the API process barely notices. Upload the same video twice and the second copy costs you nothing but a database row.

Built with FastAPI, PostgreSQL, Redis, and LocalStack standing in for S3. Comes with a working single-page frontend so you can watch the whole thing happen.

---

## What it actually does

**Bytes never pass through the API.** You ask for an upload intent, you get back presigned URLs, and your client talks to S3 directly. The API stays cheap and stateless no matter how large the files get.

**Small and large uploads take different paths, and the server decides which.** Under 50 MB you get a single presigned `PUT`. At or above 50 MB you get a multipart upload — one presigned URL per 5 MB part, uploaded in parallel, completed with the ETags your client collected. The client doesn't guess; it reads `upload_type` off the response and does what it's told.

**Folders are a real tree, not a string column.** Every folder stores its materialised path in a PostgreSQL `ltree` column — `root.documents.projects.q4` — with a GiST index over it. Recursive listing is one indexed `<@` predicate. Deleting a sub-tree is one statement. Moving a folder rewrites the path prefix of its entire sub-tree in a single `UPDATE`, no recursive CTE anywhere.

**Deduplication is content-addressed and happens off the request path.** Once an object lands, a worker streams it back out of S3 in chunks, computes SHA-256 without ever holding the whole file in memory, and checks a blob ledger. Digest already known? The new record is soft-linked to the existing object, the redundant copy is deleted from S3, and the blob's refcount goes up. Digest is new? It becomes the canonical copy. Either way, memory usage stays flat whether the file is 2 KB or 200 GB.

**Nothing is deleted until nothing points at it.** Files are soft-deleted; the underlying S3 object goes away only when the last reference to its blob does.

---

## Getting it running

You need Docker and about two minutes.

```bash
docker compose up --build
```

That brings up five containers: the API, the worker, Postgres (with `ltree` enabled before the first migration runs), Redis, and LocalStack. Migrations apply automatically, the `user-uploads` bucket is created with a browser-friendly CORS policy, and the worker waits for the schema before it starts consuming.

When it settles:

| | |
|---|---|
| Web UI | http://localhost:8000 |
| Interactive API docs | http://localhost:8000/docs |
| S3 (LocalStack) | http://localhost:4566 |

Drag a file onto the drop zone and watch it move through `UPLOADING → PROCESSING → READY`. Drop the same file again under a different name and the badge comes back marked as deduplicated — same object underneath, one row of storage billed.

There's a `Makefile` if you'd rather not type compose commands:

```bash
make up        # start everything
make logs      # tail the api and worker
make psql      # drop into the database
make bucket    # list what's actually in S3
make test      # run the suite
make down      # stop and wipe volumes
```

---

## The endpoint split that trips everyone up

Two S3 endpoints are configured, and the difference is load-bearing:

- `S3_ENDPOINT_URL=http://localstack:4566` — what the API and worker use, resolved over the Docker network.
- `S3_PUBLIC_ENDPOINT_URL=http://localhost:4566` — what presigned URLs are signed against.

SigV4 folds the `Host` header into the signature. If the API signed a URL for `localstack:4566` and handed it to your browser, the browser couldn't resolve the host — and even if it could, the signature would be computed over the wrong host and S3 would reject it. So server-side calls use the internal name and anything destined for a browser is signed against the name a browser can actually reach.

Against real AWS you drop both variables and boto3 talks to the real endpoint.

---

## How an upload actually flows

**Small file, under the threshold:**

1. `POST /api/v1/files/upload-intent` with `{name, size, mime_type, folder_id}`. You get back a `file_id` and one presigned `PUT` URL.
2. Your client `PUT`s the bytes at S3. Nothing touches the API.
3. `POST /api/v1/files/complete-upload` with `{file_id, parts: []}`. The API re-heads the object to get the authoritative size, flips the row to `PROCESSING`, and pushes a `file:uploaded` job onto Redis.

**Large file, at or above the threshold:**

1. Same intent call. This time the response carries a `multipart` block: an `upload_id`, the chunk size, and a presigned URL per part.
2. Your client slices the file into chunk-sized blobs and `PUT`s them concurrently, keeping each part's `ETag`.
3. `POST /api/v1/files/complete-upload` with all the part numbers and ETags. The API calls `CompleteMultipartUpload`, then enqueues the same job.

Either way the job queue takes over from there:

- **`file:uploaded`** — stream the object, hash it, claim or join a blob, delete the redundant copy if it's a duplicate, then enqueue the next job.
- **`file:metadata`** — read just the first few dozen bytes to categorise the file and pull image dimensions out of the header, write the metadata blob, set status `READY`.

Statuses are honest about failure too. If the object went missing between the presign and the hash, the file lands in `FAILED` with a reason attached rather than being retried forever.

---

## The queue is not just `LPUSH`/`RPOP`

A worker that crashes mid-job shouldn't lose the job. So:

- Claiming a job is an atomic `BLMOVE` onto a processing list, not a destructive pop.
- Acknowledging removes it from that list. Until then, it's still recorded as in-flight.
- Failures are separated into retryable and permanent. Retryable failures go onto a delayed sorted set with exponential backoff; permanent ones (malformed payload, row no longer exists) go straight to the dead-letter list instead of burning attempts.
- Jobs stranded on a processing list by a killed worker get reclaimed.

`GET /api/v1/queue` reports live depth across all four keys, and the UI polls it so you can see work draining in real time.

---

## API reference

Everything is under `/api/v1`.

### Files

| Method | Path | What it's for |
|---|---|---|
| `POST` | `/files/upload-intent` | Reserve a file row, get presigned URLs back |
| `POST` | `/files/complete-upload` | Finalise the upload and queue processing |
| `GET` | `/files` | List with `folder_id`, `recursive`, `status`, `search`, `limit`, `offset` |
| `GET` | `/files/stats` | Storage accounting, including what dedup saved |
| `GET` | `/files/{id}` | One file with its metadata |
| `GET` | `/files/{id}/download` | Presigned GET, only once the file is `READY` |
| `POST` | `/files/{id}/abort` | Cancel an in-flight multipart upload |
| `DELETE` | `/files/{id}` | Soft delete, releasing the blob reference |

### Folders

| Method | Path | What it's for |
|---|---|---|
| `POST` | `/folders` | Create a folder under a parent |
| `GET` | `/folders` | Flat list of every folder |
| `GET` | `/folders/tree` | Nested tree, ready to render |
| `GET` | `/folders/root` | The owner's root, created lazily on first touch |
| `GET` | `/folders/{id}` | One folder |
| `GET` | `/folders/{id}/children` | Immediate children only |
| `GET` | `/folders/{id}/subtree` | Everything beneath it, any depth |
| `GET` | `/folders/{id}/breadcrumbs` | Ancestor trail, root first |
| `PATCH` | `/folders/{id}` | Rename or move — rewrites the sub-tree's paths |
| `DELETE` | `/folders/{id}` | Delete the folder and everything under it |

### Operational

| Method | Path | What it's for |
|---|---|---|
| `GET` | `/health` | Liveness — no dependencies touched |
| `GET` | `/health/ready` | Readiness — pings Postgres, Redis and S3 |
| `GET` | `/api/v1/config` | Thresholds and limits, so clients don't hardcode them |
| `GET` | `/api/v1/queue` | Live queue depth |

Errors come back as a consistent envelope — `{"error": {"code", "message", "details"}}` — with the status code you'd expect: `400` for bad input, `404` for missing rows, `409` for state conflicts, `413` for oversized files.

---

## Configuration

Copy `.env.example` to `.env` and adjust. Compose passes these in directly, so the file is mainly for running outside Docker.

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://filestore:filestore@postgres:5432/filestore` | Must use the asyncpg driver; a plain `postgresql://` URL is rewritten for you |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `S3_ENDPOINT_URL` | `http://localstack:4566` | Server-side. Unset it to use real AWS |
| `S3_PUBLIC_ENDPOINT_URL` | `http://localhost:4566` | The host signed into browser-facing URLs |
| `S3_BUCKET` | `user-uploads` | |
| `MULTIPART_THRESHOLD_BYTES` | `52428800` (50 MB) | Above this, uploads go multipart |
| `MULTIPART_CHUNK_SIZE_BYTES` | `5242880` (5 MB) | S3's floor is 5 MB; validated at startup |
| `PRESIGN_EXPIRY_SECONDS` | `3600` | Clamped to 60s–7d |
| `MAX_FILE_SIZE_BYTES` | 100 GB | Rejected at intent time with a `413` |
| `WORKER_CONCURRENCY` | `4` | Jobs handled in parallel per worker process |
| `JOB_MAX_ATTEMPTS` | `5` | Then the job is dead-lettered |
| `HASH_STREAM_CHUNK_BYTES` | `8388608` (8 MB) | The only memory the hasher ever holds |

Bad combinations fail loudly at startup rather than at 3am — a chunk size below S3's minimum, or a threshold below the chunk size, won't boot.

---

## Tests

The suite is genuinely integration-level. It runs against real Postgres, real Redis, and LocalStack — no mocked S3, no fake queue. Presigned URLs are exercised by actually `PUT`ting bytes at them over HTTP.

```bash
make test
```

Or directly, if the services are already up:

```bash
pytest -v --cov=app --cov-report=term-missing
```

What's covered:

- **`test_upload_direct.py`** — the small-file path end to end, size validation, the `409` on double completion.
- **`test_upload_multipart.py`** — a real multipart upload with real part ETags, plus part-number validation and abort.
- **`test_folders.py`** — creation, sibling name collisions, depth limits, recursive listing, sub-tree delete, and moving a folder with children.
- **`test_worker_dedup.py`** — the pipeline itself: a unique file reaching `READY` with the right digest, a duplicate being soft-linked while its redundant object is deleted, refcounts surviving deletion in either order, and image dimensions being read from a PNG header.
- **`test_health.py`** — the probes.

The fixtures apply migrations, truncate between tests, and create the bucket, so a run is reproducible from an empty stack.

---

## CI

`.github/workflows/ci.yml` runs three jobs on every push and pull request:

- **Lint** — `ruff check` and `ruff format --check`.
- **Integration tests** — boots Postgres, Redis and LocalStack as services, enables `ltree`, waits for S3 to answer its health endpoint, applies migrations, creates the bucket, then runs the full suite with coverage.
- **Docker build** — builds the image with layer caching, validates the compose file, and confirms both entrypoints import cleanly.

---

## Layout

```
app/
  api/routes/       files, folders, health, config and queue endpoints
  core/             structured JSON logging, typed error hierarchy
  db/               async engine, session factory, the ltree column type
  models/           SQLAlchemy models: File, Folder, StorageBlob, enums
  schemas/          Pydantic request and response models
  services/         S3 presigning, the Redis queue, folder and file logic
  worker/           the consumer loop and its two jobs
migrations/         Alembic, async env
static/index.html   the whole frontend, one file
scripts/            entrypoint, LocalStack and Postgres init, bucket bootstrap
tests/              integration suite
```

A few things worth knowing if you're reading the code:

- **One image, two entrypoints.** The API and worker run the same image with a different argument, so a job can never execute against a different revision than the API that queued it.
- **`ltree` needs a codec.** asyncpg doesn't know the type natively, so a connection hook registers one and the column type carries `bind_expression`/`column_expression` fallbacks. This is the part most `ltree` integrations get wrong.
- **Blob claiming is race-safe.** `INSERT ... ON CONFLICT DO NOTHING RETURNING` means two workers hashing identical bytes at the same instant resolve cleanly — one insert wins, the loser reads the winner's row.
- **Duplicate deletion is committed first.** The database transaction lands before the redundant S3 object is deleted. If the delete fails, you leak an object and log about it; you never end up with rows pointing at bytes that aren't there.

---

## The frontend

`static/index.html` is served at `/` — one file, vanilla JS, Tailwind from a CDN. No build step, no `node_modules`.

It reads `/api/v1/config` on load so the threshold and chunk size come from the server rather than being duplicated in the client. Small uploads use `XMLHttpRequest` for real progress events; large ones slice the `File` into blobs, upload them with bounded concurrency and per-part retries, and report an aggregate percentage. Failed parts retry individually instead of restarting the whole file, and cancelling aborts the multipart reservation so S3 doesn't hold onto orphaned parts.

The explorer polls every three seconds, so you see badges flip from `PROCESSING` to `READY` as the worker gets through the queue, along with live storage totals and how much dedup has saved you.

---

## Notes and limits

Auth is deliberately out of scope — there's a single configurable owner ID, and every query is already scoped by `owner_id`, so dropping real authentication in front means resolving that value from a token instead of settings. Nothing else needs to change.

Metadata extraction reads image dimensions from PNG and GIF headers with `struct`. It doesn't shell out to ImageMagick or ffmpeg, because pulling those into the image for a header read isn't worth it; if you want richer extraction, the job is the obvious place to put it.

The `deploy.replicas` on the worker is set to 1 for a tidy local run, but nothing in the design stops you raising it. The queue claim is atomic and blob insertion handles conflicts, so workers scale horizontally as-is.

---

## License

MIT — see [LICENSE](LICENSE).






