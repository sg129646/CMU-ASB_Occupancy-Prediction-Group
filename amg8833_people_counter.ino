/*
 * AMG8833 Door-Sill People Counter for ESP32
 * ============================================
 * Sensor mounted FACE-DOWN on a door sill, looking at feet/legs passing through.
 *
 * Strategy:
 *   The 8x8 sensor is oriented so that COLUMNS 0-3 = "Room Side" and
 *   COLUMNS 4-7 = "Hallway Side" (or vice-versa — flip INVERT_DIRECTION).
 *
 *   Each frame we:
 *     1. Threshold the thermal image to find "hot blobs" (body heat).
 *     2. Run a simple connected-components (blob) finder.
 *     3. Track blobs frame-to-frame by nearest-centroid matching.
 *     4. When a tracked blob disappears, inspect which half it first
 *        appeared in vs. which half it was last seen in → direction.
 *
 * Edge cases handled:
 *   - Multiple people entering at the same time  → each is a separate blob
 *   - People entering & exiting simultaneously   → blobs tracked independently
 *   - Merging blobs (two people walking side-by-side) → treated as one event,
 *     conservatively counted once each direction
 *   - Stale blobs (person stood still too long)  → expired after BLOB_TIMEOUT_MS
 *   - Sensor noise / transient hot spots         → min-area filter on blobs
 *
 * Libraries required (install via Arduino Library Manager):
 *   - Adafruit AMG88xx  (search "Adafruit AMG88xx")
 *   - Adafruit BusIO    (dependency, usually auto-installed)
 *
 * Wiring (ESP32 DevKit):
 *   AMG8833 VIN  → 3.3V
 *   AMG8833 GND  → GND
 *   AMG8833 SDA  → GPIO 21
 *   AMG8833 SCL  → GPIO 22
 *   (INT pin not used)
 */

#include <Wire.h>
#include <Adafruit_AMG88xx.h>

// ─────────────────────────────────────────────────────────────
//  CONFIGURATION — tweak these for your environment
// ─────────────────────────────────────────────────────────────

// Temperature threshold above background to count as "body present".
// Typical body surface temp through clothing ≈ 28-33 °C.
// Set this to ambient + some margin. 3-5 °C usually works well indoors.
#define TEMP_THRESHOLD_ABOVE_BG   4.0f   // °C above rolling background

// Minimum number of pixels a blob must contain to be considered real.
// Filters out sensor noise and small animals.
#define MIN_BLOB_PIXELS           2

// Maximum pixels — a single person rarely covers more than 20 pixels
// when viewed from above at sill height (≈ 30-60 cm).
#define MAX_BLOB_PIXELS           30

// How long (ms) to keep tracking a blob after it leaves the sensor frame.
// Handles brief occlusion / sensor drop-outs.
#define BLOB_TIMEOUT_MS           800

// Max distance (in sensor pixels) to associate a new blob with an existing track.
#define MAX_TRACK_DISTANCE        3.0f

// The sensor column that divides "Room" from "Hallway".
// Columns 0-3 = one side, 4-7 = other side.
// Flip INVERT_DIRECTION if your counts are reversed.
#define DIVIDING_COLUMN           4
#define INVERT_DIRECTION          false  // set true to swap IN/OUT

// Background update rate: how quickly the rolling average adapts.
// 0.0 = never updates, 1.0 = instant. 0.02 works well.
#define BG_ALPHA                  0.02f

// Sensor frame rate: AMG8833 supports 1 Hz or 10 Hz.
// 10 Hz gives smoother tracking.
#define FRAME_RATE_10HZ           true

// Serial baud rate
#define SERIAL_BAUD               115200

// Print the raw 8x8 grid on each frame (useful for calibration/debug).
// Disable in production to reduce serial noise.
#define DEBUG_PRINT_GRID          false

// Print blob detections
#define DEBUG_PRINT_BLOBS         true

// Emit a compact single-line pixel dump each frame for the Python viewer.
// Format:  PIXELS:<p0>,<p1>,...,<p63>|BG:<b0>,<b1>,...,<b63>
// Disable if you don't need the viewer (saves ~4 KB/s of serial bandwidth).
#define PIXEL_STREAM              true

// ─────────────────────────────────────────────────────────────
//  INTERNAL STRUCTURES
// ─────────────────────────────────────────────────────────────

struct Blob {
  float cx;      // centroid x (column, 0-7)
  float cy;      // centroid y (row, 0-7)
  int   size;    // pixel count
};

struct Track {
  float cx, cy;            // current centroid
  float startCx;           // centroid x when track was first created
  bool  active;            // currently visible this frame
  unsigned long lastSeen;  // millis() timestamp
  bool  counted;           // have we already emitted a count for this track?
  int   id;
};

// ─────────────────────────────────────────────────────────────
//  GLOBALS
// ─────────────────────────────────────────────────────────────

Adafruit_AMG88xx amg;

float pixels[AMG88xx_PIXEL_ARRAY_SIZE];   // raw readings
float background[AMG88xx_PIXEL_ARRAY_SIZE]; // rolling background model
bool  visited[AMG88xx_PIXEL_ARRAY_SIZE];  // scratch for flood-fill

static const int MAX_BLOBS  = 8;
static const int MAX_TRACKS = 8;

Blob   blobs[MAX_BLOBS];
int    blobCount = 0;

Track  tracks[MAX_TRACKS];
int    trackIdCounter = 0;

volatile int peopleIn  = 0;
volatile int peopleOut = 0;

// ─────────────────────────────────────────────────────────────
//  HELPERS
// ─────────────────────────────────────────────────────────────

inline int idx(int row, int col) { return row * 8 + col; }

float euclidean(float ax, float ay, float bx, float by) {
  float dx = ax - bx, dy = ay - by;
  return sqrt(dx * dx + dy * dy);
}

// ─────────────────────────────────────────────────────────────
//  BLOB DETECTION  (connected-components on thresholded image)
// ─────────────────────────────────────────────────────────────

// Iterative flood-fill (avoids stack overflow on small MCUs)
// Returns pixel count of the blob starting at (row, col).
// Accumulates sum of col and row for centroid.
int floodFill(int startRow, int startCol, bool mask[64],
              float &sumRow, float &sumCol) {
  // Stack-based BFS
  int stackR[64], stackC[64];
  int top = 0;
  stackR[top] = startRow;
  stackC[top] = startCol;
  top++;
  visited[idx(startRow, startCol)] = true;
  int count = 0;
  sumRow = 0; sumCol = 0;

  while (top > 0) {
    top--;
    int r = stackR[top];
    int c = stackC[top];
    count++;
    sumRow += r;
    sumCol += c;

    // 4-connected neighbors
    const int dr[] = {-1, 1,  0, 0};
    const int dc[] = { 0, 0, -1, 1};
    for (int d = 0; d < 4; d++) {
      int nr = r + dr[d];
      int nc = c + dc[d];
      if (nr < 0 || nr >= 8 || nc < 0 || nc >= 8) continue;
      int ni = idx(nr, nc);
      if (!visited[ni] && mask[ni]) {
        visited[ni] = true;
        if (top < 63) {
          stackR[top] = nr;
          stackC[top] = nc;
          top++;
        }
      }
    }
  }
  return count;
}

void detectBlobs(float threshold) {
  blobCount = 0;
  bool mask[64];
  memset(visited, 0, sizeof(visited));

  for (int i = 0; i < 64; i++) {
    mask[i] = (pixels[i] - background[i]) > threshold;
  }

  for (int r = 0; r < 8; r++) {
    for (int c = 0; c < 8; c++) {
      int i = idx(r, c);
      if (mask[i] && !visited[i]) {
        float sumRow = 0, sumCol = 0;
        int size = floodFill(r, c, mask, sumRow, sumCol);
        if (size >= MIN_BLOB_PIXELS && size <= MAX_BLOB_PIXELS) {
          if (blobCount < MAX_BLOBS) {
            blobs[blobCount].cx   = sumCol / size;
            blobs[blobCount].cy   = sumRow / size;
            blobs[blobCount].size = size;
            blobCount++;
          }
        }
      }
    }
  }
}

// ─────────────────────────────────────────────────────────────
//  BLOB TRACKING  (greedy nearest-neighbour matching)
// ─────────────────────────────────────────────────────────────

void updateTracks() {
  unsigned long now = millis();

  // Mark all tracks as not-yet-matched this frame
  for (int t = 0; t < MAX_TRACKS; t++) {
    tracks[t].active = false;
  }

  bool blobMatched[MAX_BLOBS] = {};

  // Match each existing track to nearest blob
  for (int t = 0; t < MAX_TRACKS; t++) {
    if (tracks[t].id == 0) continue;  // empty slot
    if (now - tracks[t].lastSeen > BLOB_TIMEOUT_MS) {
      // Track timed out — decide direction before removing
      if (!tracks[t].counted) {
        decideDirection(tracks[t]);
      }
      tracks[t].id = 0;  // free the slot
      continue;
    }

    float bestDist = MAX_TRACK_DISTANCE + 1;
    int   bestBlob = -1;
    for (int b = 0; b < blobCount; b++) {
      if (blobMatched[b]) continue;
      float d = euclidean(tracks[t].cx, tracks[t].cy,
                          blobs[b].cx,  blobs[b].cy);
      if (d < bestDist) {
        bestDist = d;
        bestBlob = b;
      }
    }

    if (bestBlob >= 0) {
      blobMatched[bestBlob]  = true;
      tracks[t].cx           = blobs[bestBlob].cx;
      tracks[t].cy           = blobs[bestBlob].cy;
      tracks[t].active       = true;
      tracks[t].lastSeen     = now;
    }
  }

  // Create new tracks for unmatched blobs
  for (int b = 0; b < blobCount; b++) {
    if (blobMatched[b]) continue;
    // Find a free slot
    for (int t = 0; t < MAX_TRACKS; t++) {
      if (tracks[t].id == 0) {
        tracks[t].cx       = blobs[b].cx;
        tracks[t].cy       = blobs[b].cy;
        tracks[t].startCx  = blobs[b].cx;
        tracks[t].active   = true;
        tracks[t].lastSeen = now;
        tracks[t].counted  = false;
        tracks[t].id       = ++trackIdCounter;
        break;
      }
    }
  }
}

// Called when a track is about to be deleted.
// Compares where the blob started vs. ended to determine direction.
void decideDirection(Track &t) {
  // "Room side" = columns < DIVIDING_COLUMN
  bool startedInRoom   = (t.startCx < DIVIDING_COLUMN);
  bool endedInRoom     = (t.cx      < DIVIDING_COLUMN);

  if (startedInRoom == endedInRoom) {
    // Blob didn't cross — ignore (person paused, or sensor artifact)
    return;
  }

  bool movedToRoom = endedInRoom;  // started in hallway, ended in room

  if (INVERT_DIRECTION) movedToRoom = !movedToRoom;

  if (movedToRoom) {
    peopleIn++;
    Serial.printf("[COUNT] >>> ENTER  (track #%d)  | In: %d  Out: %d\n",
                  t.id, peopleIn, peopleOut);
  } else {
    peopleOut++;
    Serial.printf("[COUNT] <<< EXIT   (track #%d)  | In: %d  Out: %d\n",
                  t.id, peopleIn, peopleOut);
  }
  t.counted = true;
}

// ─────────────────────────────────────────────────────────────
//  DEBUG HELPERS
// ─────────────────────────────────────────────────────────────

void printPixelStream() {
  // Single line: PIXELS:v,v,...|BG:v,v,...|BLOBS:cx,cy;cx,cy;...
  Serial.print("PIXELS:");
  for (int i = 0; i < 64; i++) {
    Serial.print(pixels[i], 1);
    if (i < 63) Serial.print(',');
  }
  Serial.print("|BG:");
  for (int i = 0; i < 64; i++) {
    Serial.print(background[i], 1);
    if (i < 63) Serial.print(',');
  }
  Serial.print("|BLOBS:");
  for (int b = 0; b < blobCount; b++) {
    Serial.print(blobs[b].cx, 2);
    Serial.print(',');
    Serial.print(blobs[b].cy, 2);
    if (b < blobCount - 1) Serial.print(';');
  }
  Serial.println();
}


void printGrid() {
  Serial.println("-- Thermal Grid (degC above bg) --");
  for (int r = 0; r < 8; r++) {
    for (int c = 0; c < 8; c++) {
      float delta = pixels[idx(r, c)] - background[idx(r, c)];
      Serial.printf("%+5.1f ", delta);
    }
    Serial.println();
  }
  Serial.println("----------------------------------");
}

void printBlobs() {
  if (blobCount == 0) return;
  Serial.printf("[BLOBS] %d detected: ", blobCount);
  for (int b = 0; b < blobCount; b++) {
    Serial.printf("(cx=%.1f cy=%.1f sz=%d) ",
                  blobs[b].cx, blobs[b].cy, blobs[b].size);
  }
  Serial.println();
}

// ─────────────────────────────────────────────────────────────
//  SETUP
// ─────────────────────────────────────────────────────────────

void setup() {
  Serial.begin(SERIAL_BAUD);
  delay(500);
  Serial.println("\n===  AMG8833 People Counter  ===");

  Wire.begin();  // SDA=21, SCL=22 on ESP32 DevKit

  if (!amg.begin()) {
    Serial.println("ERROR: Could not find AMG8833! Check wiring.");
    while (1) { delay(500); }
  }
  Serial.println("AMG8833 found.");

  // Set frame rate
  if (FRAME_RATE_10HZ) {
    amg.setFrameRate(AMG88xx_FPS_10);
    Serial.println("Frame rate: 10 Hz");
  } else {
    amg.setFrameRate(AMG88xx_FPS_1);
    Serial.println("Frame rate: 1 Hz");
  }

  // ── Warm up background model ──────────────────────────────
  // Read several frames and average them before going live.
  Serial.print("Warming up background model");
  memset(background, 0, sizeof(background));
  const int warmupFrames = 30;
  for (int f = 0; f < warmupFrames; f++) {
    amg.readPixels(pixels);
    for (int i = 0; i < 64; i++) {
      background[i] += pixels[i] / warmupFrames;
    }
    delay(100);
    if (f % 5 == 0) Serial.print(".");
  }
  Serial.println(" done.");
  Serial.println("Sensor ready. Counting started.\n");

  memset(tracks, 0, sizeof(tracks));
}

// ─────────────────────────────────────────────────────────────
//  MAIN LOOP
// ─────────────────────────────────────────────────────────────

void loop() {
  amg.readPixels(pixels);

  // Update rolling background (only pixels NOT above threshold,
  // so a standing person doesn't permanently shift the background).
  for (int i = 0; i < 64; i++) {
    if ((pixels[i] - background[i]) < TEMP_THRESHOLD_ABOVE_BG) {
      background[i] = (1.0f - BG_ALPHA) * background[i]
                    +         BG_ALPHA  * pixels[i];
    }
  }

  // Detect blobs
  detectBlobs(TEMP_THRESHOLD_ABOVE_BG);

  // Update tracks
  updateTracks();

  // Debug output
  if (DEBUG_PRINT_GRID)   printGrid();
  if (DEBUG_PRINT_BLOBS)  printBlobs();
  if (PIXEL_STREAM)       printPixelStream();

  // AMG8833 at 10 Hz → read every 100 ms
  delay(FRAME_RATE_10HZ ? 100 : 1000);
}
