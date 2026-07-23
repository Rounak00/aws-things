/**
 * Download an entire S3 bucket to a local folder, preserving the same
 * folder structure as the S3 keys (e.g. "images/2024/jan/photo.jpg"
 * becomes <dest>/<bucket-name>/images/2024/jan/photo.jpg).
 *
 * Config comes from .env (see .env.example) and can be overridden with
 * CLI flags, e.g.:
 *   node download-bucket.js --bucket my-bucket --region eu-west-1 --prefix logs/ --concurrency 10
 */

require("dotenv").config();

const {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
} = require("@aws-sdk/client-s3");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { pipeline } = require("stream/promises");

// file-type is ESM-only; cache the dynamic import so we don't re-import per file
let fileTypePromise;
function getFileType() {
  if (!fileTypePromise) fileTypePromise = import("file-type");
  return fileTypePromise;
}

// ---------------------- CONFIG ----------------------
function parseArgs() {
  const args = {};
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg.startsWith("--")) {
      const key = arg.slice(2);
      const next = argv[i + 1];
      if (next === undefined || next.startsWith("--")) {
        args[key] = true;
      } else {
        args[key] = next;
        i++;
      }
    }
  }
  return args;
}

const cli = parseArgs();

const BUCKET_NAME = cli.bucket || process.env.BUCKET_NAME;
const AWS_REGION = cli.region || process.env.AWS_REGION;
const PREFIX = cli.prefix || process.env.PREFIX || "";
const CONCURRENCY = Number(cli.concurrency || process.env.CONCURRENCY || 5);
const MAX_RETRIES = Number(cli["max-retries"] || process.env.MAX_RETRIES || 3);

// Local destination. Defaults to <homedir>/Downloads/<bucket>, or override
// with DEST_DIR in .env / --dest on the CLI.
const DEST_DIR =
  cli.dest ||
  process.env.DEST_DIR ||
  path.join(os.homedir(), "Downloads", BUCKET_NAME || "");

if (!BUCKET_NAME || !AWS_REGION) {
  console.error(
    "Missing required config. Set BUCKET_NAME and AWS_REGION in .env (see .env.example) " +
      "or pass --bucket and --region on the command line."
  );
  process.exit(1);
}
// -----------------------------------------------------

const s3 = new S3Client({ region: AWS_REGION });

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Get every object key in the bucket (handles pagination automatically)
async function listAllKeys() {
  const keys = [];
  let continuationToken = undefined;

  do {
    const command = new ListObjectsV2Command({
      Bucket: BUCKET_NAME,
      Prefix: PREFIX || undefined,
      ContinuationToken: continuationToken,
    });
    const response = await s3.send(command);

    (response.Contents || []).forEach((obj) => {
      // Skip "folder marker" objects (keys ending in "/" with size 0)
      if (obj.Key && !obj.Key.endsWith("/")) {
        keys.push(obj.Key);
      }
    });

    continuationToken = response.IsTruncated
      ? response.NextContinuationToken
      : undefined;
  } while (continuationToken);

  return keys;
}

// Strip a "[proxy]" marker some keys carry in their filename (case-insensitive)
function stripProxyMarker(key) {
  const dir = path.posix.dirname(key);
  const base = path.posix.basename(key).replace(/\[proxy\]/gi, "").trim();
  return dir === "." ? base : `${dir}/${base}`;
}

// Download a single key to its mirrored local path, retrying on failure
async function downloadKey(key) {
  let localPath = path.join(DEST_DIR, stripProxyMarker(key));
  const localDir = path.dirname(localPath);

  await fs.promises.mkdir(localDir, { recursive: true });

  let lastErr;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const command = new GetObjectCommand({ Bucket: BUCKET_NAME, Key: key });
      const response = await s3.send(command);

      const tmpPath = `${localPath}.part`;
      const writeStream = fs.createWriteStream(tmpPath);
      await pipeline(response.Body, writeStream);

      // If the key has no extension, sniff the downloaded bytes for the real
      // file type instead of trusting the (often generic) Content-Type header.
      if (!path.extname(localPath)) {
        const { fileTypeFromFile } = await getFileType();
        const detected = await fileTypeFromFile(tmpPath);
        localPath = `${localPath}.${detected ? detected.ext : "bin"}`;
      }

      await fs.promises.rename(tmpPath, localPath);
      return;
    } catch (err) {
      lastErr = err;
      if (attempt < MAX_RETRIES) {
        await sleep(500 * attempt); // simple backoff: 500ms, 1000ms, ...
      }
    }
  }
  throw lastErr;
}

// Simple concurrency limiter so we don't open thousands of connections/file handles at once
async function runWithConcurrency(items, limit, worker, onResult) {
  let index = 0;
  let active = 0;
  let completed = 0;

  return new Promise((resolve) => {
    function next() {
      if (completed === items.length) return resolve();

      while (active < limit && index < items.length) {
        const currentIndex = index++;
        active++;
        worker(items[currentIndex])
          .then(() => {
            active--;
            completed++;
            onResult(items[currentIndex], null, completed, items.length);
            next();
          })
          .catch((err) => {
            active--;
            completed++;
            onResult(items[currentIndex], err, completed, items.length);
            next(); // keeps going even if one file fails
          });
      }
    }
    next();
  });
}

async function main() {
  console.log(`Bucket:      ${BUCKET_NAME}`);
  console.log(`Region:      ${AWS_REGION}`);
  console.log(`Prefix:      ${PREFIX || "(entire bucket)"}`);
  console.log(`Destination: ${DEST_DIR}`);
  console.log(`Concurrency: ${CONCURRENCY}  Max retries: ${MAX_RETRIES}`);
  console.log("Listing objects...");

  const keys = await listAllKeys();
  console.log(`Found ${keys.length} files. Starting download...\n`);

  const failures = [];
  const startTime = Date.now();

  await runWithConcurrency(keys, CONCURRENCY, downloadKey, (key, err, completed, total) => {
    if (err) {
      failures.push({ key, message: err.message });
      console.error(`[${completed}/${total}] FAILED: ${key} -> ${err.message}`);
    } else if (completed % 25 === 0 || completed === total) {
      console.log(`[${completed}/${total}] downloaded`);
    }
  });

  const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\nDone in ${elapsedSec}s. ${keys.length - failures.length}/${keys.length} files downloaded to:\n${DEST_DIR}`);

  if (failures.length) {
    console.log(`\n${failures.length} file(s) failed after ${MAX_RETRIES} attempts:`);
    failures.forEach((f) => console.log(`  - ${f.key}: ${f.message}`));
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
