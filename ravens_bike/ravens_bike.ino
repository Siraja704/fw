/*
 * Ravens bike ESP32 — GY-91 (MPU-9250 + BMP280), GPS, DS18B20, BLE pairing, MQTT over GSM (SIM900A).
 *
 * Install these libraries (Arduino Library Manager or ZIP):
 *   - MPU9250_asukiaaa          (MPU-9250 accel / gyro / mag on GY-91)
 *   - Adafruit BMP280 Library   (+ Adafruit Unified Sensor dependency)
 *   - TinyGSM                   (TinyGsmClient — TCP)
 *   - PubSubClient              (MQTT over TCP)
 *   - TinyGPS++
 *   - OneWire, DallasTemperature
 *   - NimBLE-Arduino
 *
 * GY-91 wiring (I2C — replaces MPU6050 block; same bus, two chip addresses):
 *   VCC -> 3.3V, GND -> GND, SDA -> GPIO 21, SCL -> GPIO 22
 *
 * SIM900A vs SIM800L (UART concept is the same; power is not):
 *   ESP32 TX -> modem RX, ESP32 RX <- modem TX, common GND.
 *   SIM900A boards usually expect ~5 V supply and brief 2 A peaks; SIM800L is often run from a 3.7–4.2 V cell.
 *   If your modem RX is strict 5 V TTL, use a level shifter on ESP32 TX->modem RX; many breakout boards are 3.3 V tolerant.
 *   Some SIM900 boards need PWR_KEY held/pulsed — follow your module’s datasheet.
 *
 * MQTT (no TLS) on 1883: uses your Oracle/Mosquitto public IP.
 */

/* PubSubClient builds the full MQTT PUBLISH frame into one buffer.
 * IMPORTANT: `#define MQTT_MAX_PACKET_SIZE` in the sketch often does NOT affect the library's compiled .cpp.
 * Reliable fix is `mqtt.setBufferSize(...)` (PubSubClient 2.8+). */
#define MQTT_BUFFER_SIZE 512

/** Enable plain MQTT (Oracle/Mosquitto :1883). */
#define ENABLE_HIVEMQ_MQTT 1

/** After GPRS, open test TCP sockets on spare muxes (see gprsTcpDiagnostics). Set 1 only while debugging TCP/DNS; leave 0 to avoid mux contention on some SIM900 firmwares. */
#define RUN_GPRS_TCP_DIAG 0

/** GPS / GY-91 / BMP280 / DS18B20 / relay / link status on USB Serial at this interval (ms). Set 0 to disable lines (telemetry interval stays 3s). */
#define SERIAL_MODULE_DEBUG_MS 3000

/** Minimum time between MQTT connect attempts when disconnected (non-blocking so Serial hardware lines still print). */
#define MQTT_RETRY_INTERVAL_MS 8000UL

#define TINY_GSM_MODEM_SIM800
#include <TinyGsmClient.h>

#include <PubSubClient.h>
#include <Preferences.h>
#include <mbedtls/sha256.h>
#include <esp_ota_ops.h>
#include <TinyGPS++.h>
#include <Wire.h>
#include <MPU9250_asukiaaa.h>
#include <Adafruit_BMP280.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <NimBLEDevice.h>
#include "esp_mac.h"

// --- Cellular APN: Zong Pakistan (4G data) ---
const char apn[] = "zonginternet";
const char gprsUser[] = "";
const char gprsPass[] = "";

// --- MQTT (Oracle Cloud / Mosquitto, plain TCP :1883) ---
// If you enable mosquitto password auth, set these. Leave empty strings for no auth.
const char* mqtt_user = "";
const char* mqtt_pass = "";
const char* MQTT_SERVER = "92.4.93.13";
const uint16_t MQTT_PORT = 1883;

// --- Firmware update (OTA) ---
// Host a manifest at: http://updates.hyperlogic.studio/fw/ravens_bike/latest.json
// that points to a .bin: http://updates.hyperlogic.studio/fw/ravens_bikeVx.y.z.bin
static const char* FW_VERSION = "1.0.0";
static const char* FW_HOST = "updates.hyperlogic.studio";
static const uint16_t FW_PORT = 80; // HTTP. If you require HTTPS, you must use a TLS-capable client.
static const char* FW_MANIFEST_PATH = "/fw/ravens_bike/latest.json";
static const unsigned long FW_CHECK_INTERVAL_MS = 24UL * 60UL * 60UL * 1000UL; // once per day

// MQTT topics (must match the Expo app)
static const char* MQTT_TOPIC_TELEMETRY = "bike/telemetry";
static const char* MQTT_TOPIC_COMMAND = "bike/command";
static const char* MQTT_TOPIC_EVENT = "bike/event";

// GATT UUIDs — must match lib/ble/constants.ts (Ravens app)
static const char* PAIRING_SERVICE_UUID = "6E400001-B5A3-F393-E0A9-E50E24DCCA9E";
static const char* HARDWARE_ID_CHAR_UUID = "6E400002-B5A3-F393-E0A9-E50E24DCCA9E";

// --- Pins ---
#define RELAY_PIN 26
#define TEMP_PIN 4
// Emergency physical override button (ESP32 GPIO35 is input-only; it has NO internal pullup/pulldown).
// Wiring recommendation: GPIO35 -> button -> GND, and add an external ~10k pull-up resistor from GPIO35 to 3.3V.
#define EMERGENCY_BUTTON_PIN 35
// UART1: GPS (unchanged)
#define GPS_RX 18
#define GPS_TX 19
// UART2: SIM900A — modem TX -> ESP32 RX, modem RX -> ESP32 TX (crossover). Avoid pins used above (not 4,18,19,21,22,26).
#define MODEM_RX 16
#define MODEM_TX 17
#define MODEM_BAUD 9600

HardwareSerial SerialGPS(1);
HardwareSerial SerialAT(2);

TinyGsm modem(SerialAT);
TinyGsmClient gsmClient(modem);
PubSubClient mqtt(gsmClient);
Preferences prefs;

static unsigned long lastFwCheckMs = 0;
static bool otaPendingVerify = false;
static unsigned long otaBootMs = 0;

/** Long TCP timeouts help slow 2G / flaky DNS before BearSSL runs (TinyGsmClient). */
static void configureGsmTcpTimeouts() {
  gsmClient.setTimeout(120000UL);
}

static void printMqttState(const __FlashStringHelper* label) {
  Serial.print(label);
  Serial.print(F(" mqtt.state()="));
  Serial.println(mqtt.state());
}

static int compareSemver(const String& a, const String& b) {
  // returns -1 if a<b, 0 if equal, 1 if a>b
  int ai[4] = {0, 0, 0, 0};
  int bi[4] = {0, 0, 0, 0};
  auto parse = [](const String& s, int out[4]) {
    int idx = 0;
    int start = 0;
    for (int i = 0; i <= (int)s.length() && idx < 4; i++) {
      if (i == (int)s.length() || s[i] == '.') {
        out[idx++] = s.substring(start, i).toInt();
        start = i + 1;
      }
    }
  };
  parse(a, ai);
  parse(b, bi);
  for (int i = 0; i < 4; i++) {
    if (ai[i] < bi[i]) return -1;
    if (ai[i] > bi[i]) return 1;
  }
  return 0;
}

static String jsonGetString(const String& json, const char* key) {
  // Very small JSON extractor for flat {"key":"value"} fields (no escapes).
  const String pat = String("\"") + key + "\":";
  int i = json.indexOf(pat);
  if (i < 0) return "";
  i += pat.length();
  while (i < (int)json.length() && (json[i] == ' ')) i++;
  if (i >= (int)json.length() || json[i] != '\"') return "";
  i++;
  int j = json.indexOf('\"', i);
  if (j < 0) return "";
  return json.substring(i, j);
}

static bool httpGetToString(const char* host, uint16_t port, const char* path, String& outBody, int maxBodyBytes = 4096) {
  TinyGsmClient c(modem, 2);
  if (!c.connect(host, port, 45)) {
    Serial.println(F("HTTP connect failed"));
    return false;
  }
  c.print(String("GET ") + path + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n");
  // status line
  String status = c.readStringUntil('\n');
  status.trim();
  if (!status.startsWith("HTTP/1.1 200") && !status.startsWith("HTTP/1.0 200")) {
    Serial.print(F("HTTP bad status: "));
    Serial.println(status);
    c.stop();
    return false;
  }
  // headers
  while (c.connected()) {
    String line = c.readStringUntil('\n');
    if (line == "\r" || line.length() == 0) break;
  }
  outBody = "";
  unsigned long start = millis();
  while (millis() - start < 60000UL) {
    while (c.available()) {
      char ch = (char)c.read();
      if ((int)outBody.length() < maxBodyBytes) outBody += ch;
    }
    if (!c.connected()) break;
    delay(10);
  }
  c.stop();
  outBody.trim();
  return outBody.length() > 0;
}

static bool sha256HexEquals(const uint8_t hash[32], const String& hexLowerOrUpper) {
  if (hexLowerOrUpper.length() != 64) return false;
  auto hexval = [](char c) -> int {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
  };
  for (int i = 0; i < 32; i++) {
    int hi = hexval(hexLowerOrUpper[i * 2]);
    int lo = hexval(hexLowerOrUpper[i * 2 + 1]);
    if (hi < 0 || lo < 0) return false;
    uint8_t b = (uint8_t)((hi << 4) | lo);
    if (b != hash[i]) return false;
  }
  return true;
}

static bool otaDownloadAndFlash(const char* host, uint16_t port, const String& binPath, const String& expectedSha256) {
  const esp_partition_t* updatePart = esp_ota_get_next_update_partition(nullptr);
  if (!updatePart) {
    Serial.println(F("No OTA partition available (check partition scheme supports OTA)"));
    return false;
  }

  TinyGsmClient c(modem, 2);
  if (!c.connect(host, port, 60)) {
    Serial.println(F("BIN HTTP connect failed"));
    return false;
  }
  c.print(String("GET ") + binPath + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n");

  String status = c.readStringUntil('\n');
  status.trim();
  if (!status.startsWith("HTTP/1.1 200") && !status.startsWith("HTTP/1.0 200")) {
    Serial.print(F("BIN HTTP bad status: "));
    Serial.println(status);
    c.stop();
    return false;
  }

  // headers + content-length
  int contentLength = -1;
  while (c.connected()) {
    String line = c.readStringUntil('\n');
    if (line == "\r" || line.length() == 0) break;
    line.trim();
    if (line.startsWith("Content-Length:")) {
      contentLength = line.substring(String("Content-Length:").length()).toInt();
    }
  }

  esp_ota_handle_t handle = 0;
  esp_err_t err = esp_ota_begin(updatePart, OTA_SIZE_UNKNOWN, &handle);
  if (err != ESP_OK) {
    Serial.print(F("esp_ota_begin failed: "));
    Serial.println((int)err);
    c.stop();
    return false;
  }

  mbedtls_sha256_context sha;
  mbedtls_sha256_init(&sha);
  // Arduino ESP32 core often exposes non-`_ret` mbedTLS APIs.
  mbedtls_sha256_starts(&sha, 0);

  uint8_t buf[1024];
  int written = 0;
  unsigned long start = millis();

  while (millis() - start < 180000UL) {
    int n = c.readBytes(buf, sizeof(buf));
    if (n > 0) {
      err = esp_ota_write(handle, buf, n);
      if (err != ESP_OK) {
        Serial.print(F("esp_ota_write failed: "));
        Serial.println((int)err);
        esp_ota_end(handle);
        c.stop();
        return false;
      }
      mbedtls_sha256_update(&sha, buf, n);
      written += n;
    } else {
      if (!c.connected() && !c.available()) break;
      delay(10);
    }
  }
  c.stop();

  uint8_t hash[32];
  mbedtls_sha256_finish(&sha, hash);
  mbedtls_sha256_free(&sha);

  if (contentLength > 0 && written != contentLength) {
    Serial.print(F("BIN length mismatch written="));
    Serial.print(written);
    Serial.print(F(" expected="));
    Serial.println(contentLength);
    esp_ota_end(handle);
    return false;
  }

  if (expectedSha256.length() == 64 && !sha256HexEquals(hash, expectedSha256)) {
    Serial.println(F("BIN SHA256 mismatch; aborting OTA"));
    esp_ota_end(handle);
    return false;
  }

  err = esp_ota_end(handle);
  if (err != ESP_OK) {
    Serial.print(F("esp_ota_end failed: "));
    Serial.println((int)err);
    return false;
  }
  err = esp_ota_set_boot_partition(updatePart);
  if (err != ESP_OK) {
    Serial.print(F("esp_ota_set_boot_partition failed: "));
    Serial.println((int)err);
    return false;
  }
  return true;
}

static void publishFwEvent(const String& hid, const String& type, const String& detail) {
  if (!mqtt.connected()) return;
  String json = "{";
  json += "\"hardware_id\":\"" + hid + "\",";
  json += "\"type\":\"fw_" + type + "\",";
  json += "\"detail\":\"" + detail + "\",";
  json += "\"ver\":\"" + String(FW_VERSION) + "\"";
  json += "}";
  mqtt.publish(MQTT_TOPIC_EVENT, json.c_str());
}

TinyGPSPlus gps;
MPU9250_asukiaaa mpu9250;
Adafruit_BMP280 bmp;
OneWire oneWire(TEMP_PIN);
DallasTemperature sensors(&oneWire);

String gHardwareId;
bool bmpOk = false;

/** 12-char uppercase hex STA MAC — stable hardware_id for BLE pairing + Supabase (no WiFi required). */
String macHexString() {
  uint8_t m[6];
  esp_read_mac(m, ESP_MAC_WIFI_STA);
  char buf[13];
  snprintf(buf, sizeof(buf), "%02X%02X%02X%02X%02X%02X", m[0], m[1], m[2], m[3], m[4], m[5]);
  return String(buf);
}

void setupBlePairing(const String& hardwareId) {
  gHardwareId = hardwareId;
  String bleName = hardwareId.length() >= 6 ? String("RAVEN-") + hardwareId.substring(hardwareId.length() - 6)
                                           : String("RAVEN-BIKE");

  NimBLEDevice::init(bleName.c_str());
  NimBLEServer* server = NimBLEDevice::createServer();

  NimBLEService* service = server->createService(PAIRING_SERVICE_UUID);
  NimBLECharacteristic* hwChar = service->createCharacteristic(
    HARDWARE_ID_CHAR_UUID,
    NIMBLE_PROPERTY::READ
  );
  hwChar->setValue(gHardwareId.c_str());
  service->start();

  NimBLEAdvertising* adv = NimBLEDevice::getAdvertising();
  adv->addServiceUUID(PAIRING_SERVICE_UUID);
  adv->start();

  Serial.print("BLE pairing ready, name=");
  Serial.print(bleName);
  Serial.print(" hardware_id=");
  Serial.println(gHardwareId);
}

bool connectGprs() {
  Serial.print("GPRS attach: APN=");
  Serial.println(apn);
  if (!modem.gprsConnect(apn, gprsUser, gprsPass)) {
    Serial.println("GPRS connect failed");
    return false;
  }
  Serial.print("GPRS OK, IP=");
  Serial.println(modem.getLocalIP());
  return true;
}

/** Plain TCP tests (no TLS). MQTT uses mux 0; probes use mux 1 and 2. */
void gprsTcpDiagnostics() {
#if RUN_GPRS_TCP_DIAG
  Serial.println(F("--- GPRS TCP diagnostics (plain TCP, not MQTT/TLS yet) ---"));

  {
    TinyGsmClient probe(modem, 1);
    Serial.println(F("Probe A: example.com:80 ..."));
    if (probe.connect("example.com", 80, 60)) {
      Serial.println(F("Probe A OK (DNS + outbound TCP work)"));
      probe.stop();
    } else {
      Serial.println(F("Probe A FAIL — no general internet path (APN/DNS/signal/carrier)"));
    }
  }

  {
    TinyGsmClient probe(modem, 2);
    Serial.print(F("Probe B: "));
    Serial.print(MQTT_SERVER);
    Serial.print(F(":"));
    Serial.print(MQTT_PORT);
    Serial.println(F(" ..."));
    if (probe.connect(MQTT_SERVER, MQTT_PORT, 90)) {
      Serial.println(F("Probe B OK (TCP to HiveMQ port — TLS can run next)"));
      probe.stop();
    } else {
      Serial.println(F("Probe B FAIL — cannot reach broker:port (blocked port, DNS, or slow 2G timeout). MQTT will fail."));
    }
  }
  Serial.println(F("--- end diagnostics ---"));
#endif
}

#if SERIAL_MODULE_DEBUG_MS > 0
void printSerialModuleDebug() {
  Serial.println(F("========== SENSORS (local) =========="));
  Serial.print(F("GPS fix: "));
  Serial.print(gps.location.isValid() ? F("yes") : F("no"));
  Serial.print(F("  sats: "));
  Serial.print(gps.satellites.value());
  Serial.print(F("  lat: "));
  Serial.print(gps.location.lat(), 6);
  Serial.print(F("  lng: "));
  Serial.print(gps.location.lng(), 6);
  Serial.print(F("  km/h: "));
  Serial.println(gps.speed.kmph(), 1);

  Serial.print(F("GY-91 accel: "));
  Serial.print(mpu9250.accelX(), 2);
  Serial.print(F(", "));
  Serial.print(mpu9250.accelY(), 2);
  Serial.print(F(", "));
  Serial.println(mpu9250.accelZ(), 2);
  Serial.print(F("GY-91 gyro:  "));
  Serial.print(mpu9250.gyroX(), 2);
  Serial.print(F(", "));
  Serial.print(mpu9250.gyroY(), 2);
  Serial.print(F(", "));
  Serial.println(mpu9250.gyroZ(), 2);
  Serial.print(F("GY-91 mag:   "));
  Serial.print(mpu9250.magX(), 2);
  Serial.print(F(", "));
  Serial.print(mpu9250.magY(), 2);
  Serial.print(F(", "));
  Serial.println(mpu9250.magZ(), 2);

  if (bmpOk) {
    float t = bmp.readTemperature();
    float p = bmp.readPressure() / 100.0f;
    Serial.print(F("BMP280 T: "));
    Serial.print(t, 2);
    Serial.print(F(" C  P: "));
    Serial.print(p, 1);
    Serial.println(F(" hPa"));
  } else {
    Serial.println(F("BMP280: (not present)"));
  }

  Serial.print(F("DS18B20: "));
  Serial.print(sensors.getTempCByIndex(0));
  Serial.println(F(" C"));

  Serial.print(F("RELAY (ign): "));
  Serial.println(digitalRead(RELAY_PIN) ? F("ON") : F("OFF"));

  Serial.print(F("GPRS: "));
  Serial.print(modem.isGprsConnected() ? F("up") : F("down"));
  Serial.print(F("  MQTT: "));
  Serial.println(mqtt.connected() ? F("connected") : F("disconnected"));
  Serial.println(F("====================================="));
}
#endif

void setup() {
  Serial.begin(115200);
  delay(800);
  Serial.println();
  Serial.println("=== RAVENS ESP32 BOOT (GY-91 + SIM900A + telemetry) ===");
  pinMode(RELAY_PIN, OUTPUT);
  digitalWrite(RELAY_PIN, LOW);
  pinMode(EMERGENCY_BUTTON_PIN, INPUT);

  Wire.begin(21, 22);
  mpu9250.beginAccel();
  mpu9250.beginGyro();
  mpu9250.beginMag();

  if (bmp.begin(0x76) || bmp.begin(0x77)) {
    bmpOk = true;
  } else {
    Serial.println("BMP280 not found (check GY-91 I2C)");
  }

  sensors.begin();
  SerialGPS.begin(9600, SERIAL_8N1, GPS_RX, GPS_TX);

  SerialAT.begin(MODEM_BAUD, SERIAL_8N1, MODEM_RX, MODEM_TX);
  delay(1000);
  Serial.println("Modem init...");
  modem.restart();
  Serial.println(modem.getModemInfo());

  String hid = macHexString();
  Serial.println("----------------------------------------");
  Serial.print("HARDWARE_ID (copy into Supabase): ");
  Serial.println(hid);
  Serial.println("----------------------------------------");

  setupBlePairing(hid);

  if (!connectGprs()) {
    Serial.println("Will retry GPRS in loop.");
  } else {
#if RUN_GPRS_TCP_DIAG
    gprsTcpDiagnostics();
#endif
    configureGsmTcpTimeouts();
    delay(2500);
    Serial.println(F("GPRS settled. MQTT uses Oracle/Mosquitto on :1883."));
  }

  mqtt.setServer(MQTT_SERVER, MQTT_PORT);
  mqtt.setCallback(mqttCallback);
  mqtt.setSocketTimeout(30);
  mqtt.setKeepAlive(60);
  /* PubSubClient 2.8+ supports setBufferSize; without it publish() will fail for payloads > ~128 bytes. */
  mqtt.setBufferSize(MQTT_BUFFER_SIZE);

  prefs.begin("ravens", false);

  // OTA rollback support: if we booted into a new image pending verification,
  // mark it valid only after we prove basic health (GPRS + MQTT stay up).
  esp_ota_img_states_t st;
  const esp_partition_t* running = esp_ota_get_running_partition();
  if (running && esp_ota_get_state_partition(running, &st) == ESP_OK && st == ESP_OTA_IMG_PENDING_VERIFY) {
    otaPendingVerify = true;
    otaBootMs = millis();
    Serial.println(F("OTA image pending verify (rollback enabled). Will mark valid after health check."));
  }

#if SERIAL_MODULE_DEBUG_MS > 0
  Serial.println(F("--- hardware snapshot (then every SERIAL_MODULE_DEBUG_MS in loop) ---"));
  while (SerialGPS.available() > 0) gps.encode(SerialGPS.read());
  mpu9250.accelUpdate();
  mpu9250.gyroUpdate();
  mpu9250.magUpdate();
  sensors.requestTemperatures();
  delay(750);
  printSerialModuleDebug();
#endif
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String msg;
  for (unsigned int i = 0; i < length; i++) msg += (char)payload[i];

  if (msg == "START") digitalWrite(RELAY_PIN, HIGH);
  else if (msg == "STOP") digitalWrite(RELAY_PIN, LOW);
  else if (msg == "FWCHECK") {
    // Force an immediate firmware check on next loop.
    lastFwCheckMs = 0;
  }
}

static String pendingEmergencyEventJson;

static void tryPublishJsonEvent(const String& json) {
  if (!mqtt.connected()) return;
  const bool ok = mqtt.publish(MQTT_TOPIC_EVENT, json.c_str());
  if (ok) Serial.println(F("Published emergency event (bike/event)"));
  else Serial.println(F("FAILED to publish emergency event (bike/event)"));
}

static void handleEmergencyButton(const String& hid) {
  // Requirement:
  // - First press starts a 10s window.
  // - Exactly 5 presses within that window toggles relay immediately.
  // - If presses exceed 5 (6/7/...) within the window, CANCEL and ignore until the window ends.
  // - After a successful toggle, apply a 5s cooldown (ignore presses).
  //
  // Button wiring (recommended): pulled HIGH, press pulls to GND (LOW).
  static int lastStable = HIGH;
  static int lastReading = HIGH;
  static unsigned long lastDebounceMs = 0;

  enum State : uint8_t { IDLE = 0, COUNTING = 1, LOCKOUT = 2, COOLDOWN = 3 };
  static State st = IDLE;
  static int pressCount = 0;
  static unsigned long windowStartMs = 0;
  static unsigned long lockoutUntilMs = 0;
  static unsigned long cooldownUntilMs = 0;

  const unsigned long now = millis();

  // Time-based transitions (handle even with no new button edges).
  if (st == COUNTING && windowStartMs != 0 && (now - windowStartMs) > 10000UL) {
    // Window expired without success -> reset.
    st = IDLE;
    pressCount = 0;
    windowStartMs = 0;
  }
  if (st == LOCKOUT && lockoutUntilMs != 0 && (long)(now - lockoutUntilMs) >= 0) {
    st = IDLE;
    pressCount = 0;
    windowStartMs = 0;
    lockoutUntilMs = 0;
  }
  if (st == COOLDOWN && cooldownUntilMs != 0 && (long)(now - cooldownUntilMs) >= 0) {
    st = IDLE;
    cooldownUntilMs = 0;
  }

  const int reading = digitalRead(EMERGENCY_BUTTON_PIN);

  // Debounce (50ms)
  if (reading != lastReading) {
    lastDebounceMs = now;
    lastReading = reading;
  }
  if ((now - lastDebounceMs) < 50) return;
  if (reading == lastStable) return;
  lastStable = reading;

  // Count press on falling edge (HIGH -> LOW)
  if (lastStable != LOW) return;

  if (st == COOLDOWN || st == LOCKOUT) {
    // Ignore presses during cooldown/lockout.
    return;
  }

  if (st == IDLE) {
    st = COUNTING;
    pressCount = 0;
    windowStartMs = now;
    Serial.println(F("Emergency sequence started (10s window)"));
  }

  // COUNTING
  pressCount++;
  Serial.print(F("Emergency button press count="));
  Serial.println(pressCount);

  if (pressCount > 5) {
    // Cancel until the 10s window ends.
    st = LOCKOUT;
    lockoutUntilMs = windowStartMs + 10000UL;
    Serial.println(F("Emergency sequence CANCELLED (too many presses)"));
    return;
  }

  if (pressCount < 5) return;

  // Exactly 5 presses -> toggle immediately
  const bool relayWasOn = digitalRead(RELAY_PIN) == HIGH;
  const bool relayNowOn = !relayWasOn;
  digitalWrite(RELAY_PIN, relayNowOn ? HIGH : LOW);
  Serial.print(F("EMERGENCY TOGGLE RELAY -> "));
  Serial.println(relayNowOn ? F("ON") : F("OFF"));

  // Build event payload (publish when MQTT is available; local toggle works always)
  String json = "{";
  json += "\"hardware_id\":\"" + hid + "\",";
  json += "\"type\":\"emergency_relay\",";
  json += "\"action\":\"" + String(relayNowOn ? "ON" : "OFF") + "\",";
  json += "\"presses\":5,";
  json += "\"ign\":\"" + String(relayNowOn ? "ON" : "OFF") + "\"";
  if (gps.location.isValid()) {
    json += ",\"lat\":" + String(gps.location.lat(), 6);
    json += ",\"lng\":" + String(gps.location.lng(), 6);
  }
  json += "}";

  pendingEmergencyEventJson = json;
  tryPublishJsonEvent(pendingEmergencyEventJson);
  if (mqtt.connected()) pendingEmergencyEventJson = "";

  // Success -> cooldown 5 seconds and reset state.
  st = COOLDOWN;
  cooldownUntilMs = now + 5000UL;
  pressCount = 0;
  windowStartMs = 0;
}

static float accelMagG() {
  const float ax = mpu9250.accelX();
  const float ay = mpu9250.accelY();
  const float az = mpu9250.accelZ();
  return sqrtf(ax * ax + ay * ay + az * az);
}

static float gyroMagDps() {
  const float gx = mpu9250.gyroX();
  const float gy = mpu9250.gyroY();
  const float gz = mpu9250.gyroZ();
  return sqrtf(gx * gx + gy * gy + gz * gz);
}

static void publishAccidentEventIfNeeded(const String& hid) {
  // Simple production-friendly heuristics: impact (accel spike) + cooldown.
  // Thresholds should be tuned after collecting real rides + potholes.
  static unsigned long lastEventMs = 0;
  const unsigned long now = millis();
  if (lastEventMs != 0 && (now - lastEventMs) < 30000UL) return; // 30s cooldown

  const float a = accelMagG();
  const float g = gyroMagDps();

  const bool impact = (a > 3.0f);          // >3g spike
  const bool violentRotation = (g > 250.0f); // dps
  if (!impact && !violentRotation) return;

  lastEventMs = now;

  String json = "{";
  json += "\"hardware_id\":\"" + hid + "\",";
  json += "\"type\":\"impact\",";
  json += "\"severity\":" + String(impact && violentRotation ? 5 : impact ? 4 : 3) + ",";
  json += "\"a_g\":" + String(a, 2) + ",";
  json += "\"g_dps\":" + String(g, 1) + ",";
  if (gps.location.isValid()) {
    json += "\"lat\":" + String(gps.location.lat(), 6) + ",";
    json += "\"lng\":" + String(gps.location.lng(), 6) + ",";
  }
  json += "\"ign\":\"" + String(digitalRead(RELAY_PIN) ? "ON" : "OFF") + "\"";
  json += "}";

  if (mqtt.connected()) {
    const bool ok = mqtt.publish(MQTT_TOPIC_EVENT, json.c_str());
    if (ok) Serial.println(F("Published accident event (bike/event)"));
    else Serial.println(F("FAILED to publish accident event (bike/event)"));
  }
}

void loop() {
  static unsigned long lastGprsRetry = 0;
  if (!modem.isGprsConnected()) {
    if (millis() - lastGprsRetry > 20000) {
      lastGprsRetry = millis();
      connectGprs();
    }
  } else {
    if (!mqtt.connected()) {
      tryMqttReconnect();
    }
    if (mqtt.connected()) {
      mqtt.loop();
    }
  }

  // OTA verify: if we are in a pending-verify image, mark valid after 60s of stability + MQTT connected.
  if (otaPendingVerify) {
    if (modem.isGprsConnected() && mqtt.connected() && (millis() - otaBootMs) > 60000UL) {
      esp_err_t ok = esp_ota_mark_app_valid_cancel_rollback();
      Serial.print(F("Mark OTA image valid: "));
      Serial.println(ok == ESP_OK ? F("OK") : F("FAIL"));
      otaPendingVerify = false;
    }
  }

  while (SerialGPS.available() > 0) gps.encode(SerialGPS.read());

  static unsigned long lastSample = 0;
  const unsigned long samplePeriod =
      (SERIAL_MODULE_DEBUG_MS > 0) ? (unsigned long)SERIAL_MODULE_DEBUG_MS : 3000UL;
  if (millis() - lastSample < samplePeriod) return;
  lastSample = millis();

  mpu9250.accelUpdate();
  mpu9250.gyroUpdate();
  mpu9250.magUpdate();
  sensors.requestTemperatures();

#if SERIAL_MODULE_DEBUG_MS > 0
  printSerialModuleDebug();
#endif

  if (!modem.isGprsConnected()) {
    return;
  }

  String hid = macHexString();

  // Always allow emergency local override, even if MQTT is down.
  handleEmergencyButton(hid);
  if (pendingEmergencyEventJson.length() > 0) {
    tryPublishJsonEvent(pendingEmergencyEventJson);
    if (mqtt.connected()) pendingEmergencyEventJson = "";
  }

  // Daily firmware check (also forced by MQTT command "FWCHECK").
  if (lastFwCheckMs == 0 || (millis() - lastFwCheckMs) > FW_CHECK_INTERVAL_MS) {
    lastFwCheckMs = millis();
    prefs.putULong("fw_last_ms", lastFwCheckMs);

    String manifest;
    Serial.println(F("FW: checking latest.json ..."));
    if (httpGetToString(FW_HOST, FW_PORT, FW_MANIFEST_PATH, manifest, 4096)) {
      const String latest = jsonGetString(manifest, "latest");
      const String binUrl = jsonGetString(manifest, "bin");
      const String sha = jsonGetString(manifest, "sha256");
      if (latest.length() > 0 && compareSemver(String(FW_VERSION), latest) < 0) {
        Serial.print(F("FW: update available -> "));
        Serial.println(latest);
        publishFwEvent(hid, "available", latest);

        // Expect bin url like: http://updates.hyperlogic.studio/fw/ravens_bikeV1.2.3.bin
        String path = binUrl;
        if (path.startsWith("http://")) {
          int slash = path.indexOf('/', 7);
          if (slash > 0) path = path.substring(slash);
        }
        if (!path.startsWith("/")) {
          Serial.println(F("FW: invalid bin path in manifest"));
        } else {
          publishFwEvent(hid, "start", latest);
          const bool ok = otaDownloadAndFlash(FW_HOST, FW_PORT, path, sha);
          if (ok) {
            publishFwEvent(hid, "rebooting", latest);
            Serial.println(F("FW: flashed OK, rebooting..."));
            delay(1500);
            ESP.restart();
          } else {
            publishFwEvent(hid, "failed", latest);
            Serial.println(F("FW: update failed"));
          }
        }
      }
    } else {
      Serial.println(F("FW: manifest fetch failed"));
    }
  }

  float prPa = 0, bmpTemp = NAN;
  if (bmpOk) {
    bmpTemp = bmp.readTemperature();
    prPa = bmp.readPressure();
  }

  /* Keep payload small so it works even if PubSubClient buffer is left at default on some installs. */
  String json = "{";
  json += "\"hardware_id\":\"" + hid + "\",";
  json += "\"lat\":" + String(gps.location.lat(), 6) + ",";
  json += "\"lng\":" + String(gps.location.lng(), 6) + ",";
  json += "\"speed\":" + String(gps.speed.kmph(), 1) + ",";
  json += "\"temp\":" + String(sensors.getTempCByIndex(0), 2) + ",";
  json += "\"ign\":\"" + String(digitalRead(RELAY_PIN) ? "ON" : "OFF") + "\"";
  json += "}";

  if (mqtt.connected()) {
    /* Let PubSubClient process pings / inbound before a large outbound publish (GSM links are finicky). */
    mqtt.loop();
    mqtt.loop();

    const uint16_t len = (uint16_t)json.length();
    const bool pubOk = mqtt.publish(MQTT_TOPIC_TELEMETRY, json.c_str());
    if (pubOk) {
      Serial.print(F("Published telemetry (MQTT via GSM), bytes="));
      Serial.println(len);
    } else {
      Serial.print(F("PUBLISH FAILED, jsonBytes="));
      Serial.print(len);
      Serial.print(F(" (TCP write failed or internal buffer too small) "));
      printMqttState(F("after publish"));
      /* connected() can still be true while the TCP socket is half-dead; force a clean reconnect. */
      Serial.println(F("Forcing MQTT disconnect + reconnect on next loop"));
      mqtt.disconnect();
    }

    // Event channel: publish only on spikes (rare).
    publishAccidentEventIfNeeded(hid);
  } else {
    // If you see this repeatedly, the device isn't actually connected to the broker.
    printMqttState(F("not connected"));
  }
}

/** One MQTT connect attempt per call; spaced by MQTT_RETRY_INTERVAL_MS so loop() keeps running (hardware Serial lines work while MQTT is down). */
void tryMqttReconnect() {
#if !ENABLE_HIVEMQ_MQTT
  return;
#else
  if (!modem.isGprsConnected()) return;

  static unsigned long lastMqttAttempt = 0;
  unsigned long now = millis();
  if (lastMqttAttempt != 0 && (now - lastMqttAttempt) < MQTT_RETRY_INTERVAL_MS) return;
  lastMqttAttempt = now;
  configureGsmTcpTimeouts();

  Serial.print(F("MQTT connect -> "));
  Serial.print(MQTT_SERVER);
  Serial.print(F(":"));
  Serial.println(MQTT_PORT);

  String clientID = "ESP32_Bike_" + String(random(0xffff), HEX);
  bool ok;
  if (mqtt_user && mqtt_user[0] != '\0') ok = mqtt.connect(clientID.c_str(), mqtt_user, mqtt_pass);
  else ok = mqtt.connect(clientID.c_str());
  if (ok) {
    mqtt.subscribe(MQTT_TOPIC_COMMAND);
    Serial.println(F("MQTT connected (Oracle/Mosquitto)"));
  } else {
    Serial.print(F("MQTT failed, state="));
    Serial.println(mqtt.state());

    // Quick plain-TCP probe to detect carrier / firewall / routing issues.
    // This is much faster than waiting for repeated MQTT timeouts.
    TinyGsmClient probe(modem, 1);
    Serial.print(F("TCP probe -> "));
    Serial.print(MQTT_SERVER);
    Serial.print(F(":"));
    Serial.print(MQTT_PORT);
    Serial.print(F(" ... "));
    if (probe.connect(MQTT_SERVER, MQTT_PORT, 30)) {
      Serial.println(F("OK"));
      probe.stop();
    } else {
      Serial.println(F("FAIL (GPRS up but cannot reach broker port)"));
    }
  }
#endif
}
