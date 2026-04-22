#include <WiFi.h>
#include <WiFiManager.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <HTTPUpdate.h>

#define MQTT_MAX_PACKET_SIZE 1024
#include <PubSubClient.h>
#include <ArduinoJson.h>

#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <math.h>
#include <time.h>
#include <Preferences.h>
#include <vector>
#include <mbedtls/md.h>

/*
  ============================================================================
  File: esp32c3_oled_mqtt.ino
  Project: Notificator Project - ESP32-C3 OLED Notifier

  Firmware metadata
  - Name: Notificator Project Device Firmware
  - Version: 1.0.1
  - Target: ESP32-C3 SuperMini + SSD1306 OLED + TTP223 capacitive sensor
  - Transport: MQTT (TLS supported)
  - Storage: Preferences (namespace: wpnotif)

  Compatibility
  - Arduino core: ESP32
  - Display: Adafruit_SSD1306 (128x64)

  Maintenance
  - Update FW_VERSION / FW_VERSION_DATE when behavior changes.
  - Keep gesture map and command docs aligned with implementation.
  ============================================================================
*/
#define FW_NAME "Notificator Project IoT Device Firmware"
#define FW_VERSION "1.0.1"
#define FW_VERSION_DATE "2026-04-13"

// OTA security: this secret key signs OTA commands using HMAC-SHA256.
// Do not send it over MQTT. Rotate if leaked.
#define OTA_SHARED_TOKEN ""
#define OTA_REQUIRE_HTTPS true
static const unsigned long OTA_TS_MAX_SKEW_SEC = 300;

/*
  Notificator Project ESP32-C3 firmware
  --------------------------------
  Responsibilities
  - Input: merged gesture handler from physical button + TTP223 capacitive sensor.
  - Transport: MQTT message topic + MQTT command topic per deviceId.
  - UI: status bar + message viewer + idle themes (clock / weather).
  - Storage: ring buffer persisted in Preferences key "hist".

  Runtime message payload in RAM
  - Stored as: "title|body"
  - If separator is missing, content is rendered as a single text block.

  Gesture map (normal mode)
  - 1 tap: mark current message read
  - 2 taps: show next message
  - 3 taps: toggle current message read/unread
  - hold >= 2s: clear all messages
  - hold >= 6s: start setup portal
  - 4+ taps: show device id + firmware version ( single tap to get back to iddle screen )
  - 8+ capacitive-only taps: start setup portal

  Visual unread cues
  - Status bar envelope blinks when any unread message exists.
  - Message header shows explicit READ/UNREAD badge for current message.
*/

// -------------------- HW --------------------
#define OLED_WIDTH 128
#define OLED_HEIGHT 64
#define OLED_ADDR 0x3C

#define I2C_SDA 20
#define I2C_SCL 21

#define BUTTON_PIN 9
#define BUTTON_ACTIVE_LOW true
#define TTP223_PIN 0
#define BUTTON_DEBOUNCE_MS 20
#define BUTTON_LONG_PRESS_MS 2000
#define BUTTON_SETUP_LONG_PRESS_MS 6000
#define TAP_WINDOW_MS 700
#define TAP_MIN_PRESS_MS 25
#define TTP223_SETUP_TAP_COUNT 8

#define SHOW_ID_TAP_COUNT 4
#define SHOW_ID_DISPLAY_MS 4000

#define MESSAGE_BUFFER_SIZE 10

#define WIFI_AP_PREFIX "WPNOTIF-"
#define WIFI_AP_PASSWORD ""

// -------------------- MQTT --------------------
#define MQTT_USE_TLS true

const char *MQTT_HOST = "";
const uint16_t MQTT_PORT = 8883;
const char *MQTT_USERNAME = "";
const char *MQTT_PASSWORD = "";
const char *TELEMETRY_API_URL = "";
const char *TELEMETRY_API_TOKEN = "";
// Keep false in production. Set true only for temporary TLS diagnostics.
#define TELEMETRY_TLS_INSECURE false
const char *MQTT_CA_CERT = R"EOF(
-----BEGIN CERTIFICATE-----
MIIFazCCA1OgAwIBAgIRAIIQz7DSQONZRGPgu2OCiwAwDQYJKoZIhvcNAQELBQAw
TzELMAkGA1UEBhMCVVMxKTAnBgNVBAoTIEludGVybmV0IFNlY3VyaXR5IFJlc2Vh
cmNoIEdyb3VwMRUwEwYDVQQDEwxJU1JHIFJvb3QgWDEwHhcNMTUwNjA0MTEwNDM4
WhcNMzUwNjA0MTEwNDM4WjBPMQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJu
ZXQgU2VjdXJpdHkgUmVzZWFyY2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBY
MTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAK3oJHP0FDfzm54rVygc
h77ct984kIxuPOZXoHj3dcKi/vVqbvYATyjb3miGbESTtrFj/RQSa78f0uoxmyF+
0TM8ukj13Xnfs7j/EvEhmkvBioZxaUpmZmyPfjxwv60pIgbz5MDmgK7iS4+3mX6U
A5/TR5d8mUgjU+g4rk8Kb4Mu0UlXjIB0ttov0DiNewNwIRt18jA8+o+u3dpjq+sW
T8KOEUt+zwvo/7V3LvSye0rgTBIlDHCNAymg4VMk7BPZ7hm/ELNKjD+Jo2FR3qyH
B5T0Y3HsLuJvW5iB4YlcNHlsdu87kGJ55tukmi8mxdAQ4Q7e2RCOFvu396j3x+UC
B5iPNgiV5+I3lg02dZ77DnKxHZu8A/lJBdiB3QW0KtZB6awBdpUKD9jf1b0SHzUv
KBds0pjBqAlkd25HN7rOrFleaJ1/ctaJxQZBKT5ZPt0m9STJEadao0xAH0ahmbWn
OlFuhjuefXKnEgV4We0+UXgVCwOPjdAvBbI+e0ocS3MFEvzG6uBQE3xDk3SzynTn
jh8BCNAw1FtxNrQHusEwMFxIt4I7mKZ9YIqioymCzLq9gwQbooMDQaHWBfEbwrbw
qHyGO0aoSCqI3Haadr8faqU9GY/rOPNk3sgrDQoo//fb4hVC1CLQJ13hef4Y53CI
rU7m2Ys6xt0nUW7/vGT1M0NPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNV
HRMBAf8EBTADAQH/MB0GA1UdDgQWBBR5tFnme7bl5AFzgAiIyBpY9umbbjANBgkq
hkiG9w0BAQsFAAOCAgEAVR9YqbyyqFDQDLHYGmkgJykIrGF1XIpu+ILlaS/V9lZL
ubhzEFnTIZd+50xx+7LSYK05qAvqFyFWhfFQDlnrzuBZ6brJFe+GnY+EgPbk6ZGQ
3BebYhtF8GaV0nxvwuo77x/Py9auJ/GpsMiu/X1+mvoiBOv/2X/qkSsisRcOj/KK
NFtY2PwByVS5uCbMiogziUwthDyC3+6WVwW6LLv3xLfHTjuCvjHIInNzktHCgKQ5
ORAzI4JMPJ+GslWYHb4phowim57iaztXOoJwTdwJx4nLCgdNbOhdjsnvzqvHu7Ur
TkXWStAmzOVyyghqpZXjFaH3pO3JLF+l+/+sKAIuvtd7u+Nxe5AW0wdeRlN8NwdC
jNPElpzVmbUq4JUagEiuTDkHzsxHpFKVK7q4+63SM1N95R1NbdWhscdCb+ZAJzVc
oyi3B43njTOQ5yOf+1CceWxG1bQVs5ZufpsMljq4Ui0/1lvh+wjChP4kqKOJ2qxq
4RgqsahDYVvTH9w7jXbyLeiNdd8XM2w9U/t7y0Ff/9yi0GE44Za4rF2LN9d11TPA
mRGunUHBcnWEvgJBQl9nJEiU0Zsnvgc/ubhPgXRR4Xq37Z0j4r7g1SgEEzwxA57d
emyPxgcYxn/eR44/KJ4EBs+lVDR3veyJm+kXQ99b21/+jh5Xos1AnX5iItreGCc=
-----END CERTIFICATE-----
)EOF";

// Netlify (api-wpnotificator.netlify.app) trust anchor.
const char *TELEMETRY_CA_CERT = R"EOF(
-----BEGIN CERTIFICATE-----
MIIDjjCCAnagAwIBAgIQAzrx5qcRqaC7KGSxHQn65TANBgkqhkiG9w0BAQsFADBh
MQswCQYDVQQGEwJVUzEVMBMGA1UEChMMRGlnaUNlcnQgSW5jMRkwFwYDVQQLExB3
d3cuZGlnaWNlcnQuY29tMSAwHgYDVQQDExdEaWdpQ2VydCBHbG9iYWwgUm9vdCBH
MjAeFw0xMzA4MDExMjAwMDBaFw0zODAxMTUxMjAwMDBaMGExCzAJBgNVBAYTAlVT
MRUwEwYDVQQKEwxEaWdpQ2VydCBJbmMxGTAXBgNVBAsTEHd3dy5kaWdpY2VydC5j
b20xIDAeBgNVBAMTF0RpZ2lDZXJ0IEdsb2JhbCBSb290IEcyMIIBIjANBgkqhkiG
9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuzfNNNx7a8myaJCtSnX/RrohCgiN9RlUyfuI
2/Ou8jqJkTx65qsGGmvPrC3oXgkkRLpimn7Wo6h+4FR1IAWsULecYxpsMNzaHxmx
1x7e/dfgy5SDN67sH0NO3Xss0r0upS/kqbitOtSZpLYl6ZtrAGCSYP9PIUkY92eQ
q2EGnI/yuum06ZIya7XzV+hdG82MHauVBJVJ8zUtluNJbd134/tJS7SsVQepj5Wz
tCO7TG1F8PapspUwtP1MVYwnSlcUfIKdzXOS0xZKBgyMUNGPHgm+F6HmIcr9g+UQ
vIOlCsRnKPZzFBQ9RnbDhxSJITRNrw9FDKZJobq7nMWxM4MphQIDAQABo0IwQDAP
BgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwIBhjAdBgNVHQ4EFgQUTiJUIBiV
5uNu5g/6+rkS7QYXjzkwDQYJKoZIhvcNAQELBQADggEBAGBnKJRvDkhj6zHd6mcY
1Yl9PMWLSn/pvtsrF9+wX3N3KjITOYFnQoQj8kVnNeyIv/iPsGEMNKSuIEyExtv4
NeF22d+mQrvHRAiGfzZ0JFrabA0UWTW98kndth/Jsw1HKj2ZL7tcu7XUIOGZX1NG
Fdtom/DzMNU+MeKNhJ7jitralj41E6Vf8PlwUHBHQRFXGU7Aj64GxJUTFy8bJZ91
8rGOmaFvE7FBcf6IKshPECBV1/MUReXgRPTqh5Uykw7+U0b6LJ3/iyK5S9kJRaTe
pLiaWN0bfVKfjllDiIGknibVb63dDcY3fe0Dkhvld1927jyNxF1WW6LZZm6zNTfl
MrY=
-----END CERTIFICATE-----
)EOF";

// OTA TLS trust anchor (kept separate from MQTT for independent rotation).
const char *OTA_CA_CERT = MQTT_CA_CERT;
const char *OTA_ALLOWED_HOST_SUFFIX = "";

WiFiClientSecure tlsClient;
PubSubClient mqttClient(tlsClient);

unsigned long lastMqttAttemptMs = 0;
static const unsigned long MQTT_RECONNECT_MS = 2000;
unsigned long lastAcceptedOtaTs = 0;
bool telemetryBootReportPending = true;
unsigned long lastTelemetryRetryMs = 0;
static const unsigned long TELEMETRY_RETRY_MS = 30000;

// -------------------- WiFi state machine --------------------
static const unsigned long WIFI_RETRY_MS = 3500;
static const unsigned long WIFI_DRAW_MS = 600;
static const unsigned long WIFI_POST_CONNECT_GRACE_MS = 4000;
static const unsigned long WIFI_HARD_RESET_AFTER_MS = 90000; // 90s
static const unsigned long WIFI_PORTAL_AFTER_MS = 240000;	 // 4 minutes
static const unsigned long WIFI_STACK_RESET_MS = 250;

unsigned long wifiConnectingSinceMs = 0;
unsigned long lastWifiAttemptMs = 0;
unsigned long lastWifiDrawMs = 0;
unsigned long wifiConnectedAtMs = 0;
bool wifiHardResetDone = false;

// -------------------- OLED/LED --------------------
Adafruit_SSD1306 display(OLED_WIDTH, OLED_HEIGHT, &Wire, -1);

unsigned long lastHeartbeatMs = 0;
bool heartbeatOn = false;

unsigned long lastMsgLedFlashMs = 0;
static const unsigned long MSG_LED_FLASH_COOLDOWN_MS = 1200;

// -------------------- Device ID / topics --------------------
String deviceId = "";
String mqttSubTopic = "";
String mqttCmdTopic = "";
String apSsid = "";
unsigned long showIdUntilMs = 0;
bool showIdSticky = false;
unsigned long showNoMessagesUntilMs = 0;
static const unsigned long NO_MESSAGES_DISPLAY_MS = 1400;

// -------------------- WiFiManager --------------------
WiFiManager wm;
bool portalRunning = false;
volatile bool portalClientConnected = false;
bool deviceConfigured = false;

unsigned long animLastMs = 0;
uint8_t animFrame = 0;

unsigned long portalStartMs = 0;
bool portalStartChecked = false;
bool setupScreenDrawn = false;

// -------------------- Preferences --------------------
Preferences prefs;

// -------------------- Remote / UI state --------------------
// 0 = clock (default), 1 = weather+clock hybrid, 2 = weather
uint8_t idleTheme = 0;

// prevent idle UI jumps right after actions/messages
static const unsigned long NO_IDLE_AFTER_ACTION_MS = 2000;
unsigned long noIdleUntilMs = 0;

// -------------------- Geo by IP (for weather theme 2) --------------------
// Weather follows the device's public IP (the network the ESP32 is on).
static const unsigned long GEO_REFRESH_MS = 12UL * 60UL * 60UL * 1000UL; // 12 hours
unsigned long lastGeoFetchMs = 0;
bool geoHasData = false;
bool geoFetching = false;
bool geoManualOverride = false;

float geoLat = 37.9838f; // fallback Athens
float geoLon = 23.7275f; // fallback Athens
String geoCity = "ATH";
String geoTz = "Europe/Athens";

// -------------------- Weather (theme 2) --------------------
static const unsigned long WEATHER_REFRESH_MS = 15UL * 60UL * 1000UL; // 15 min
unsigned long lastWeatherFetchMs = 0;
bool weatherHasData = false;
bool weatherFetching = false;

float weatherTempC = 0;
float weatherWindKmh = 0;
uint8_t weatherCode = 255;

// -------------------- Persistent history --------------------
static const uint16_t MAX_TOPIC_CHARS = 24;
static const uint16_t MAX_PAYLOAD_CHARS = 90;

struct __attribute__((packed)) PersistMsg
{
	char topic[MAX_TOPIC_CHARS];
	char payload[MAX_PAYLOAD_CHARS];
	uint8_t read;
};

struct __attribute__((packed)) PersistHistory
{
	uint32_t magic;
	uint8_t version;
	uint8_t count;
	uint8_t head;
	uint8_t current;
	PersistMsg msgs[MESSAGE_BUFFER_SIZE];
};

static const uint32_t HISTORY_MAGIC = 0x57504E46; // 'WPNF'
static const uint8_t HISTORY_VERSION = 3;

// In-RAM history
struct MqttMessage
{
	String topic;
	String payload;
	bool read;
};

MqttMessage messageBuffer[MESSAGE_BUFFER_SIZE];
size_t messageCount = 0;
size_t messageHead = 0;
size_t currentIndex = 0;
bool historyActive = false;

bool historyDirty = false;
unsigned long historyDirtySinceMs = 0;
static const unsigned long HISTORY_SAVE_DELAY_MS = 1200;

// -------------------- Message flip --------------------
// When payload contains both title and body, alternate display phases.
static const unsigned long MESSAGE_TITLE_MS = 3200;
static const unsigned long MESSAGE_BODY_MS = 2600;
bool showTitlePhase = true;
unsigned long lastFlipMs = 0;
size_t flipIndex = (size_t)-1;

// -------------------- Unread blink --------------------
unsigned long lastUnreadBlinkMs = 0;
bool unreadBlinkOn = false;
static const unsigned long RECEIVING_BADGE_MS = 1200;
unsigned long receivingUntilMs = 0;

// -------------------- Time/NTP --------------------
bool ntpStarted = false;
bool placeholdersRestamped = false;

// -------------------- RSSI smoothing --------------------
int16_t smoothedRssi = -999;
static const float RSSI_ALPHA = 0.35f;
static const unsigned long RSSI_UPDATE_MS = 700;
unsigned long lastRssiUpdateMs = 0;

// -------------------- Button state --------------------
static bool btnStable = false;
static bool btnRawLast = false;
static unsigned long btnRawChangedMs = 0;

static bool ttpStable = false;
static bool ttpRawLast = false;
static unsigned long ttpRawChangedMs = 0;

static bool btnDown = false;
static unsigned long btnDownMs = 0;
static uint8_t ttpIdleLevel = LOW;
static bool pressStartedByBtn = false;
static bool pressStartedByTtp = false;
static bool holdPreviewActive = false;
static unsigned long holdPreviewMs = 0;
static const unsigned long HOLD_PREVIEW_START_MS = BUTTON_LONG_PRESS_MS;

static uint8_t tapCount = 0;
static bool tapPending = false;
static unsigned long tapDeadlineMs = 0;
static bool tapOnlyTtp = true;

// -------------------- Idle --------------------
static const unsigned long IDLE_AFTER_MS = 45000;
unsigned long lastUserOrMsgMs = 0;

static const unsigned long CLOCK_REDRAW_MS = 1000;
unsigned long lastClockDrawMs = 0;
static const unsigned long IDLE_HYBRID_PHASE_MS = 2500;

// -------------------- Forward decls --------------------
void drawStatusBar(bool dim = false);
void drawWrappedMessage(const String &topic, const String &message, bool unreadMark);

bool hasMessages();
uint16_t unreadCount();

void markHistoryDirty();
void maybeSaveHistory();
void saveHistoryToPrefsNow();
void loadHistoryFromPrefs();
void clearHistoryInRam();
void wipeHistoryPrefs();

bool findFirstUnreadIndex(size_t &outIdx);
void focusFirstUnreadIfAny();

void showMessageAt(size_t idx, bool markRead);
void showCurrentMessage(bool markRead);
void gotoPrevMessage(bool markRead);
void markCurrentReadAndPersist();
void toggleCurrentReadStateAndPersist();
void markAllReadAndPersist();
void clearAllMessagesAndShowFeedback();

void startSetupPortal();
void finalizeSetupAfterPortal();
void setupMqttClient();
void connectToMqtt();

void drawBootWelcomeScreen();
void drawSetupInstructions();
void drawPortalAnimationFrame();
void drawDeviceIdScreen();
void showNoMessagesOverlay();
void drawNoMessagesScreen();
void updateLedIndicator();
void drawHoldCounter(unsigned long heldMs);

bool timeReady();
String humanNow();
void restampTimePlaceholdersIfReady();
void applyDeviceTimezone();
const char *resolveTimezonePosix();

void startStaConnectStable();
void hardResetWiFiStackOnce();

int16_t readRssiRaw();
void updateSmoothedRssi();
uint8_t rssiBars();
void drawRssiBars(uint8_t bars, bool dim);

void handleButton();
void resetMessageFlipState(size_t idx);
void resetTapSequence();

// idle
// clock + weather idle themes
void drawIdleClockFrame();
void drawIdleHybridFrame();
String humanTimeHHMM();

// geo + weather
void maybeFetchGeoByIP();
bool fetchGeoByIPNow();
void maybeFetchWeather();
bool fetchWeatherNow();
void drawIdleWeatherFrame();
const char *weatherCodeToShort(uint8_t code);

// commands
void loadIdleTheme();
void saveIdleTheme(uint8_t v);
void loadWeatherConfig();
void saveWeatherConfig(bool manual);
void handleCmdJson(const String &json);
bool isValidOtaUrl(const String &url);
void performOtaUpdate(const String &url, const String &targetVersion);
bool parseVersionTriplet(const String &v, int &majorV, int &minorV, int &patchV);
bool isRemoteVersionNewer(const String &currentV, const String &remoteV);
String buildOtaSignBase(const String &url, const String &version, unsigned long ts, bool force, const String &device);
bool hmacSha256Hex(const String &key, const String &message, String &outHex);
bool publishFirmwareTelemetry(const char *eventName, const char *otaStatus = nullptr, const String &targetVersion = "", const String &error = "");

static inline void bumpNoIdleGuard()
{
	unsigned long now = millis();
	noIdleUntilMs = now + NO_IDLE_AFTER_ACTION_MS;
	lastUserOrMsgMs = now;
}

void resetTapSequence()
{
	tapCount = 0;
	tapPending = false;
	tapDeadlineMs = 0;
	tapOnlyTtp = true;
}

// -------------------- Helpers --------------------
static void safeCopyToC(char *dst, size_t dstSize, const String &src)
{
	if (!dst || dstSize == 0)
		return;
	size_t n = src.length();
	if (n >= dstSize)
		n = dstSize - 1;
	memcpy(dst, src.c_str(), n);
	dst[n] = '\0';
}

void drawStatus(const char *title, const char *line)
{
	display.clearDisplay();
	display.setTextSize(1);
	display.setTextColor(SSD1306_WHITE);
	display.setCursor(0, 0);
	display.println(title);
	if (line && line[0])
	{
		display.setCursor(0, 16);
		display.println(line);
	}
	display.display();
}

void drawCenteredText(const char *line1, const char *line2 = nullptr, uint8_t size = 2)
{
	display.clearDisplay();
	display.setTextColor(SSD1306_WHITE);

	auto centerX = [&](const char *s, uint8_t ts) -> int
	{
		int16_t x1, y1;
		uint16_t w, h;
		display.setTextSize(ts);
		display.getTextBounds(s, 0, 0, &x1, &y1, &w, &h);
		return (OLED_WIDTH - (int)w) / 2;
	};

	if (line2 && line2[0])
	{
		int yTop = 18;
		display.setTextSize(size);
		display.setCursor(centerX(line1, size), yTop);
		display.println(line1);
		display.setCursor(centerX(line2, size), yTop + (size * 12));
		display.println(line2);
	}
	else
	{
		int y = 28;
		display.setTextSize(size);
		display.setCursor(centerX(line1, size), y);
		display.println(line1);
	}

	display.display();
}

void setOnboardLed(bool on)
{
	(void)on;
}

void setOnboardLedColor(uint8_t r, uint8_t g, uint8_t b)
{
	(void)r;
	(void)g;
	(void)b;
}

void flashOnboardLedColor(uint8_t r, uint8_t g, uint8_t b, uint16_t durationMs = 60)
{
	(void)r;
	(void)g;
	(void)b;
	(void)durationMs;
}

void flashOnboardLed(uint16_t durationMs = 60)
{
	flashOnboardLedColor(255, 255, 255, durationMs);
}

void updateLedIndicator()
{
	// LED disabled to reduce firmware size.
}

void loadConfiguredFlag()
{
	prefs.begin("wpnotif", true);
	deviceConfigured = prefs.getBool("configured", false);
	prefs.end();
}

void saveConfiguredFlag(bool v)
{
	prefs.begin("wpnotif", false);
	prefs.putBool("configured", v);
	prefs.end();
}

void saveLastSsidForInfo(const String &ssid)
{
	if (!ssid.length())
		return;
	prefs.begin("wpnotif", false);
	prefs.putString("lastSsid", ssid);
	prefs.end();
}

// -------------------- Idle theme persistence --------------------
void loadIdleTheme()
{
	prefs.begin("wpnotif", true);
	idleTheme = (uint8_t)prefs.getUChar("idleTheme", 0);
	prefs.end();
	if (!(idleTheme == 0 || idleTheme == 1 || idleTheme == 2))
		idleTheme = 0;
}

void saveIdleTheme(uint8_t v)
{
	if (!(v == 0 || v == 1 || v == 2))
		v = 0;
	idleTheme = v;

	prefs.begin("wpnotif", false);
	prefs.putUChar("idleTheme", idleTheme);
	prefs.end();

	lastClockDrawMs = 0;

	lastGeoFetchMs = 0;
	geoHasData = geoManualOverride;
	geoFetching = false;

	lastWeatherFetchMs = 0;
	weatherHasData = false;
	weatherFetching = false;
}

void loadWeatherConfig()
{
	prefs.begin("wpnotif", true);
	geoManualOverride = prefs.getBool("geoManual", false);

	if (geoManualOverride)
	{
		geoLat = prefs.getFloat("geoLat", geoLat);
		geoLon = prefs.getFloat("geoLon", geoLon);
		geoCity = prefs.getString("geoCity", geoCity.c_str());
		geoTz = prefs.getString("geoTz", geoTz.c_str());
	}
	prefs.end();

	if (!geoManualOverride)
		return;

	if (!geoCity.length())
		geoCity = "GEO";
	geoCity.toUpperCase();
	if (geoCity.length() > 12)
		geoCity = geoCity.substring(0, 12);

	if (!geoTz.length())
		geoTz = "Europe/Athens";
	applyDeviceTimezone();

	geoHasData = true;
	lastGeoFetchMs = millis();
}

void saveWeatherConfig(bool manual)
{
	prefs.begin("wpnotif", false);
	prefs.putBool("geoManual", manual);

	if (manual)
	{
		prefs.putFloat("geoLat", geoLat);
		prefs.putFloat("geoLon", geoLon);
		prefs.putString("geoCity", geoCity);
		prefs.putString("geoTz", geoTz);
	}

	prefs.end();
}

const char *resolveTimezonePosix()
{
	// Default: Athens/Greece
	const char *tzPosix = "EET-2EEST,M3.5.0/3,M10.5.0/4";

	if (!geoTz.length())
		return tzPosix;

	// POSIX strings may include '/' in DST transition rules (e.g. M3.5.0/3).
	// Treat as legacy IANA only when it looks like Region/City with no comma rules.
	bool looksLikeIana = (geoTz.indexOf('/') >= 0) && (geoTz.indexOf(',') < 0);
	if (!looksLikeIana)
	{
		return geoTz.c_str();
	}

	// Backward compatibility for older IANA values saved on device.
	if (geoTz == "Europe/London" || geoTz == "Europe/Dublin" || geoTz == "Europe/Lisbon")
	{
		return "GMT0BST,M3.5.0/1,M10.5.0/2";
	}
	if (geoTz == "Europe/Athens" || geoTz == "Europe/Bucharest" || geoTz == "Europe/Sofia")
	{
		return "EET-2EEST,M3.5.0/3,M10.5.0/4";
	}
	if (geoTz == "Europe/Berlin" || geoTz == "Europe/Paris" || geoTz == "Europe/Madrid")
	{
		return "CET-1CEST,M3.5.0/2,M10.5.0/3";
	}
	return "UTC0";
}

void applyDeviceTimezone()
{
	const char *tzPosix = resolveTimezonePosix();
	setenv("TZ", tzPosix, 1);
	tzset();
}

// -------------------- Time helpers --------------------
bool timeReady()
{
	time_t now = time(nullptr);
	return now > 1700000000;
}

String humanNow()
{
	if (!timeReady())
		return "TIME:--";
	time_t now = time(nullptr);
	struct tm tm;
	localtime_r(&now, &tm);

	char buf[12];
	snprintf(buf, sizeof(buf), "%02d/%02d %02d:%02d",
			 tm.tm_mday, tm.tm_mon + 1,
			 tm.tm_hour, tm.tm_min);
	return String(buf);
}

String humanTimeHHMM()
{
	if (!timeReady())
		return "--:--";
	time_t now = time(nullptr);
	struct tm tm;
	localtime_r(&now, &tm);
	char buf[6];
	snprintf(buf, sizeof(buf), "%02d:%02d", tm.tm_hour, tm.tm_min);
	return String(buf);
}

void restampTimePlaceholdersIfReady()
{
	if (placeholdersRestamped)
		return;
	if (!timeReady())
		return;
	if (!hasMessages())
	{
		placeholdersRestamped = true;
		return;
	}

	bool changed = false;
	for (size_t i = 0; i < messageCount; i++)
	{
		size_t idx = (messageHead + i) % MESSAGE_BUFFER_SIZE;
		if (messageBuffer[idx].topic.startsWith("TIME:--"))
		{
			String t = messageBuffer[idx].topic;
			int sp = t.indexOf(' ');
			String suffix = "";
			if (sp >= 0)
				suffix = t.substring(sp);
			messageBuffer[idx].topic = humanNow() + suffix;
			changed = true;
		}
	}

	if (changed)
	{
		markHistoryDirty();
		saveHistoryToPrefsNow();
	}

	placeholdersRestamped = true;
}

// -------------------- WiFi --------------------
void startStaConnectStable()
{
	WiFi.mode(WIFI_STA);
	WiFi.setSleep(false);
	WiFi.setAutoReconnect(true);
	WiFi.begin();

	wifiConnectingSinceMs = millis();
	lastWifiAttemptMs = 0;
	wifiHardResetDone = false;
}

void hardResetWiFiStackOnce()
{
	if (wifiHardResetDone)
		return;
	wifiHardResetDone = true;

	WiFi.mode(WIFI_OFF);
	delay(WIFI_STACK_RESET_MS);

	WiFi.disconnect(true, false);
	delay(WIFI_STACK_RESET_MS);

	WiFi.mode(WIFI_STA);
	WiFi.setSleep(false);
	WiFi.setAutoReconnect(true);
	WiFi.begin();
}

// -------------------- RSSI --------------------
int16_t readRssiRaw()
{
	if (!WiFi.isConnected())
		return -999;
	return (int16_t)WiFi.RSSI();
}

void updateSmoothedRssi()
{
	unsigned long now = millis();
	if (now - lastRssiUpdateMs < RSSI_UPDATE_MS)
		return;
	lastRssiUpdateMs = now;

	int16_t raw = readRssiRaw();
	if (raw == -999)
	{
		smoothedRssi = -999;
		return;
	}

	if (smoothedRssi == -999)
		smoothedRssi = raw;
	else
		smoothedRssi = (int16_t)((RSSI_ALPHA * raw) + ((1.0f - RSSI_ALPHA) * smoothedRssi));
}

uint8_t rssiBars()
{
	if (smoothedRssi == -999)
		return 0;
	int r = smoothedRssi;
	if (r >= -50)
		return 4;
	if (r >= -60)
		return 3;
	if (r >= -70)
		return 2;
	if (r >= -80)
		return 1;
	return 0;
}

void drawRssiBars(uint8_t bars, bool dim)
{
	// Minimal icon-only signal indicator (no numeric dB text).
	const int baseX = OLED_WIDTH - 22;
	const int baseY = 11;
	const int barW = 4;
	const int gap = 1;

	for (uint8_t i = 0; i < 4; i++)
	{
		const int h = 3 + (i * 2);
		const int x = baseX + (i * (barW + gap));
		const int y = baseY - h + 1;

		if (i < bars)
		{
			if (dim)
				display.drawRect(x, y, barW, h, SSD1306_WHITE);
			else
				display.fillRect(x, y, barW, h, SSD1306_WHITE);
		}
		else
		{
			display.drawRect(x, y, barW, h, SSD1306_WHITE);
		}
	}

	(void)dim;
}

// -------------------- History persistence --------------------
void clearHistoryInRam()
{
	messageCount = 0;
	messageHead = 0;
	currentIndex = 0;
	historyActive = false;

	for (size_t i = 0; i < MESSAGE_BUFFER_SIZE; i++)
	{
		messageBuffer[i].topic = "";
		messageBuffer[i].payload = "";
		messageBuffer[i].read = true;
	}
}

void wipeHistoryPrefs()
{
	prefs.begin("wpnotif", false);
	prefs.remove("hist");
	prefs.end();
}

void markHistoryDirty()
{
	if (!historyDirty)
	{
		historyDirty = true;
		historyDirtySinceMs = millis();
	}
}

void maybeSaveHistory()
{
	if (!historyDirty)
		return;
	if (historyDirtySinceMs == 0)
		historyDirtySinceMs = millis();
	if (millis() - historyDirtySinceMs >= HISTORY_SAVE_DELAY_MS)
	{
		saveHistoryToPrefsNow();
	}
}

void saveHistoryToPrefsNow()
{
	PersistHistory ph;
	memset(&ph, 0, sizeof(ph));
	ph.magic = HISTORY_MAGIC;
	ph.version = HISTORY_VERSION;

	uint8_t countToSave = (messageCount > MESSAGE_BUFFER_SIZE)
							  ? (uint8_t)MESSAGE_BUFFER_SIZE
							  : (uint8_t)messageCount;

	ph.count = countToSave;
	ph.head = 0;

	uint8_t curPos = 0;
	if (countToSave > 0)
	{
		size_t pos = (currentIndex + MESSAGE_BUFFER_SIZE - messageHead) % MESSAGE_BUFFER_SIZE;
		if (pos >= countToSave)
			pos = 0;
		curPos = (uint8_t)pos;
	}
	ph.current = curPos;

	for (uint8_t i = 0; i < countToSave; i++)
	{
		size_t idx = (messageHead + i) % MESSAGE_BUFFER_SIZE;
		safeCopyToC(ph.msgs[i].topic, sizeof(ph.msgs[i].topic), messageBuffer[idx].topic);
		safeCopyToC(ph.msgs[i].payload, sizeof(ph.msgs[i].payload), messageBuffer[idx].payload);
		ph.msgs[i].read = messageBuffer[idx].read ? 1 : 0;
	}

	for (uint8_t i = countToSave; i < MESSAGE_BUFFER_SIZE; i++)
	{
		ph.msgs[i].topic[0] = '\0';
		ph.msgs[i].payload[0] = '\0';
		ph.msgs[i].read = 1;
	}

	prefs.begin("wpnotif", false);
	prefs.putBytes("hist", &ph, sizeof(ph));
	prefs.end();

	historyDirty = false;
	historyDirtySinceMs = 0;
}

void loadHistoryFromPrefs()
{
	PersistHistory ph;
	memset(&ph, 0, sizeof(ph));

	prefs.begin("wpnotif", true);
	size_t gotLen = prefs.getBytesLength("hist");
	if (gotLen != sizeof(PersistHistory))
	{
		prefs.end();
		clearHistoryInRam();
		return;
	}
	prefs.getBytes("hist", &ph, sizeof(ph));
	prefs.end();

	if (ph.magic != HISTORY_MAGIC || ph.version != HISTORY_VERSION)
	{
		clearHistoryInRam();
		return;
	}

	uint8_t count = ph.count;
	if (count == 0 || count > MESSAGE_BUFFER_SIZE)
	{
		clearHistoryInRam();
		return;
	}

	for (size_t i = 0; i < MESSAGE_BUFFER_SIZE; i++)
	{
		messageBuffer[i].topic = "";
		messageBuffer[i].payload = "";
		messageBuffer[i].read = true;
	}

	messageHead = 0;
	messageCount = count;
	historyActive = false;

	for (uint8_t i = 0; i < count; i++)
	{
		messageBuffer[i].topic = String(ph.msgs[i].topic);
		messageBuffer[i].payload = String(ph.msgs[i].payload);
		messageBuffer[i].read = (ph.msgs[i].read != 0);
	}

	uint8_t curPos = ph.current;
	if (curPos >= count)
		curPos = 0;
	currentIndex = curPos;

	focusFirstUnreadIfAny();
}

bool findFirstUnreadIndex(size_t &outIdx)
{
	if (messageCount == 0)
		return false;
	for (size_t i = 0; i < messageCount; i++)
	{
		size_t idx = (messageHead + i) % MESSAGE_BUFFER_SIZE;
		if (!messageBuffer[idx].read)
		{
			outIdx = idx;
			return true;
		}
	}
	return false;
}

void focusFirstUnreadIfAny()
{
	size_t idx = 0;
	if (findFirstUnreadIndex(idx))
	{
		currentIndex = idx;
		historyActive = false;
	}
}

// -------------------- Status bar --------------------
bool hasMessages() { return messageCount > 0; }

uint16_t unreadCount()
{
	uint16_t c = 0;
	for (size_t i = 0; i < messageCount; i++)
	{
		size_t idx = (messageHead + i) % MESSAGE_BUFFER_SIZE;
		if (!messageBuffer[idx].read)
			c++;
	}
	return c;
}

void updateHeartbeat()
{
	unsigned long now = millis();
	if (now - lastHeartbeatMs >= 950)
	{
		lastHeartbeatMs = now;
		heartbeatOn = !heartbeatOn;
	}
}

void updateUnreadBlink()
{
	unsigned long now = millis();
	if (now - lastUnreadBlinkMs >= 500)
	{
		lastUnreadBlinkMs = now;
		unreadBlinkOn = !unreadBlinkOn;
	}
}

void drawHeartbeatLeftBig(bool dim)
{
	// Keep a little breathing room from the top edge.
	const int cx = 5, cy = 6;
	const int r = 3;

	if (heartbeatOn)
	{
		display.fillCircle(cx, cy, r, SSD1306_WHITE);
	}
	else
	{
		display.drawCircle(cx, cy, r, SSD1306_WHITE);
	}
}

void drawUnreadEnvelopeMid(bool dim)
{
	unsigned long now = millis();
	if (now < receivingUntilMs)
	{
		display.setTextSize(1);
		display.setTextColor(SSD1306_WHITE);
		const char *badge = "RECEIVING";
		int16_t x1, y1;
		uint16_t w, h;
		display.getTextBounds(badge, 0, 0, &x1, &y1, &w, &h);
		int cx = ((OLED_WIDTH - (int)w) / 2) - x1;
		display.setCursor(cx, 2);
		display.print(badge);
		(void)dim;
		return;
	}

	if (unreadCount() == 0)
		return;

	updateUnreadBlink();
	if (!unreadBlinkOn)
		return;

	const int w = 14, h = 9;
	const int x = (OLED_WIDTH - w) / 2;
	const int y = 2;

	display.drawRect(x, y, w, h, SSD1306_WHITE);
	display.drawLine(x, y, x + w / 2, y + h / 2, SSD1306_WHITE);
	display.drawLine(x + w - 1, y, x + w / 2, y + h / 2, SSD1306_WHITE);
	display.drawLine(x, y + h - 1, x + w / 2, y + h / 2, SSD1306_WHITE);
	display.drawLine(x + w - 1, y + h - 1, x + w / 2, y + h / 2, SSD1306_WHITE);

	(void)dim;
}

void drawStatusBar(bool dim)
{
	updateSmoothedRssi();
	updateHeartbeat();

	display.setTextSize(1);
	display.setTextColor(SSD1306_WHITE);
	drawHeartbeatLeftBig(dim);
	drawUnreadEnvelopeMid(dim);
	drawRssiBars(rssiBars(), dim);
}

// -------------------- Message rendering --------------------
static uint8_t chooseTextSize(const String &message)
{
	return (message.length() <= 24) ? 2 : 1;
}

static uint8_t maxCharsForSize(uint8_t textSize)
{
	return (textSize == 2) ? 10 : 21;
}

void resetMessageFlipState(size_t idx)
{
	flipIndex = idx;
	showTitlePhase = true;
	lastFlipMs = millis();
}

// Draw one message frame with word-wrap, title/body phase logic, and optional paging.
void drawWrappedMessage(const String &topic, const String &message, bool unreadMark)
{
	if (flipIndex != currentIndex)
		resetMessageFlipState(currentIndex);

	String title = message;
	String body = "";
	int sep = message.indexOf('|');
	if (sep >= 0)
	{
		title = message.substring(0, sep);
		body = message.substring(sep + 1);
		int sep2 = body.indexOf('|');
		if (sep2 >= 0)
			body = body.substring(0, sep2);
	}

	String displayText;
	bool bodyPhaseVisible = false;
	if (title.length() && body.length())
	{
		unsigned long now = millis();
		unsigned long phaseMs = showTitlePhase ? MESSAGE_TITLE_MS : MESSAGE_BODY_MS;
		if (now - lastFlipMs >= phaseMs)
		{
			showTitlePhase = !showTitlePhase;
			lastFlipMs = now;
		}

		bodyPhaseVisible = !showTitlePhase;
		displayText = bodyPhaseVisible ? body : title;
	}
	else if (title.length())
	{
		displayText = title;
	}
	else
	{
		displayText = body;
	}
	if (!displayText.length())
		displayText = "(empty)";

	// Keep title/body readable while reserving space for the timestamp header.
	const uint8_t textSize = bodyPhaseVisible ? 2 : chooseTextSize(displayText);
	const uint8_t maxChars = maxCharsForSize(textSize);
	const uint8_t maxLines = (textSize == 2)
								 ? 2
								 : 5;

	display.clearDisplay();
	drawStatusBar(false);

	display.setTextSize(1);
	display.setTextColor(SSD1306_WHITE);
	const char *stateLabel = unreadMark ? "UNREAD" : "READ";
	int16_t lx1, ly1;
	uint16_t lw, lh;
	display.getTextBounds(stateLabel, 0, 0, &lx1, &ly1, &lw, &lh);
	int stateX = ((int)OLED_WIDTH - (int)lw - 1) - lx1;

	// Keep topic text from colliding with the right-side read/unread badge.
	String header = topic;
	int headerMaxPx = stateX - 6;
	while (header.length() > 0)
	{
		int16_t hx1, hy1;
		uint16_t hw, hh;
		display.getTextBounds(header.c_str(), 0, 0, &hx1, &hy1, &hw, &hh);
		if ((int)hw <= headerMaxPx)
			break;
		header.remove(header.length() - 1);
	}

	display.setCursor(0, 16);
	display.print(header);
	display.setCursor(stateX, 16);
	display.print(stateLabel);

	// Per-message unread marker so unread items are identifiable while browsing.
	const int unreadX = stateX - 5;
	const int unreadY = 19;
	if (unreadMark)
		display.fillCircle(unreadX, unreadY, 2, SSD1306_WHITE);
	else
		display.drawCircle(unreadX, unreadY, 2, SSD1306_WHITE);

	display.setTextSize(textSize);

	auto appendWrappedLine = [&](const String &srcLine, std::vector<String> &outLines)
	{
		String rest = srcLine;
		while (rest.length())
		{
			if (rest.length() <= maxChars)
			{
				outLines.push_back(rest);
				break;
			}

			int cut = maxChars;
			while (cut > 0 && rest[cut - 1] != ' ' && rest[cut - 1] != '\t')
				cut--;
			if (cut <= 0)
				cut = maxChars; // no whitespace found; hard split

			String chunk = rest.substring(0, cut);
			chunk.trim();
			if (chunk.length())
			{
				outLines.push_back(chunk);
			}

			int nextStart = cut;
			while (nextStart < (int)rest.length() && (rest[nextStart] == ' ' || rest[nextStart] == '\t'))
				nextStart++;
			rest = (nextStart < (int)rest.length()) ? rest.substring(nextStart) : String("");
		}
	};

	std::vector<String> wrappedLines;
	wrappedLines.reserve(10);

	int start = 0;
	while (start < (int)displayText.length())
	{
		int nl = displayText.indexOf('\n', start);
		String part = (nl >= 0) ? displayText.substring(start, nl) : displayText.substring(start);
		start = (nl >= 0) ? (nl + 1) : (int)displayText.length();

		if (!part.length())
		{
			wrappedLines.push_back("");
			continue;
		}
		appendWrappedLine(part, wrappedLines);
	}

	size_t startLine = 0;
	if (bodyPhaseVisible && textSize == 2 && wrappedLines.size() > maxLines)
	{
		const uint8_t pageStride = 2; // slight overlap between pages
		const unsigned long pageMs = 2200;
		size_t pages = 1 + (wrappedLines.size() - maxLines + pageStride - 1) / pageStride;
		size_t page = (millis() / pageMs) % pages;
		startLine = page * pageStride;
		if (startLine + maxLines > wrappedLines.size())
		{
			startLine = wrappedLines.size() - maxLines;
		}
	}

	for (size_t i = startLine; i < wrappedLines.size() && i < (startLine + maxLines); i++)
	{
		display.println(wrappedLines[i]);
	}

	display.display();
}

void showMessageAt(size_t idx, bool markRead)
{
	if (!hasMessages())
		return;

	idx %= MESSAGE_BUFFER_SIZE;
	currentIndex = idx;
	resetMessageFlipState(currentIndex);

	bool isUnread = !messageBuffer[currentIndex].read;
	if (markRead && isUnread)
	{
		messageBuffer[currentIndex].read = true;
		saveHistoryToPrefsNow();
		isUnread = false;
	}

	drawWrappedMessage(messageBuffer[currentIndex].topic, messageBuffer[currentIndex].payload, isUnread);
	bumpNoIdleGuard();
}

void showCurrentMessage(bool markRead)
{
	if (!hasMessages())
		return;
	showMessageAt(currentIndex, markRead);
}

void showNoMessagesOverlay()
{
	showNoMessagesUntilMs = millis() + NO_MESSAGES_DISPLAY_MS;
	// Keep the short feedback visible, then allow idle immediately.
	lastUserOrMsgMs = millis() - (IDLE_AFTER_MS + 1000);
	noIdleUntilMs = millis();
	drawNoMessagesScreen();
}

void drawNoMessagesScreen()
{
	drawCenteredText("NO", "MESSAGES", 2);
}

void gotoNextMessage(bool markRead)
{
	if (!hasMessages())
	{
		showNoMessagesOverlay();
		return;
	}

	historyActive = true;

	size_t pos = (currentIndex + MESSAGE_BUFFER_SIZE - messageHead) % MESSAGE_BUFFER_SIZE;
	if (pos >= messageCount)
		pos = 0;

	pos = (pos + 1) % messageCount;
	size_t idx = (messageHead + pos) % MESSAGE_BUFFER_SIZE;
	showMessageAt(idx, markRead);
}

void gotoPrevMessage(bool markRead)
{
	if (!hasMessages())
	{
		showNoMessagesOverlay();
		return;
	}

	historyActive = true;

	size_t pos = (currentIndex + MESSAGE_BUFFER_SIZE - messageHead) % MESSAGE_BUFFER_SIZE;
	if (pos >= messageCount)
		pos = 0;

	pos = (pos + messageCount - 1) % messageCount;
	size_t idx = (messageHead + pos) % MESSAGE_BUFFER_SIZE;
	showMessageAt(idx, markRead);
}

void markCurrentReadAndPersist()
{
	if (!hasMessages())
	{
		showNoMessagesOverlay();
		return;
	}

	if (!messageBuffer[currentIndex].read)
	{
		messageBuffer[currentIndex].read = true;
		saveHistoryToPrefsNow();
	}
	bumpNoIdleGuard();
	showCurrentMessage(false);
}

void toggleCurrentReadStateAndPersist()
{
	if (!hasMessages())
	{
		showNoMessagesOverlay();
		return;
	}

	messageBuffer[currentIndex].read = !messageBuffer[currentIndex].read;
	saveHistoryToPrefsNow();

	bumpNoIdleGuard();
	showCurrentMessage(false);
}

void markAllReadAndPersist()
{
	if (!hasMessages())
		return;

	bool changed = false;
	for (size_t i = 0; i < messageCount; i++)
	{
		size_t idx = (messageHead + i) % MESSAGE_BUFFER_SIZE;
		if (!messageBuffer[idx].read)
		{
			messageBuffer[idx].read = true;
			changed = true;
		}
	}
	if (changed)
		saveHistoryToPrefsNow();

	bumpNoIdleGuard();
	showCurrentMessage(false);
}

void clearAllMessagesAndShowFeedback()
{
	// Shared clear routine used by both long-press gesture and MQTT cmd clear_msgs.
	clearHistoryInRam();
	wipeHistoryPrefs();
	placeholdersRestamped = false;
	resetMessageFlipState(0);
	drawCenteredText("CLEARED", "MSGS", 2);
	flashOnboardLedColor(255, 120, 0, 50);
	flashOnboardLedColor(255, 120, 0, 50);

	// No messages left: make idle eligible immediately.
	lastUserOrMsgMs = millis() - (IDLE_AFTER_MS + 1000);
	noIdleUntilMs = millis();
}

// -------------------- Push message --------------------
static String normalizeType(const String &type)
{
	if (!type.length())
		return "";
	if (type == "generic_notification")
		return "GEN";
	if (type == "warning" || type == "warn")
		return "WARN";
	if (type == "error" || type == "err")
		return "ERR";
	if (type == "info")
		return "INFO";
	return type.length() > 6 ? type.substring(0, 6) : type;
}

static String makeHeaderWithType(const String &type)
{
	String t = humanNow();
	String tt = normalizeType(type);
	if (tt.length())
		t += " " + tt;
	return t;
}

void pushMessage(const String &header, const String &payload)
{
	size_t insertIndex;

	if (messageCount < MESSAGE_BUFFER_SIZE)
	{
		insertIndex = (messageHead + messageCount) % MESSAGE_BUFFER_SIZE;
		messageCount++;
	}
	else
	{
		insertIndex = messageHead;
		messageHead = (messageHead + 1) % MESSAGE_BUFFER_SIZE;
	}

	String t = header.length() ? header : makeHeaderWithType("");
	String p = payload;

	if (t.length() >= (MAX_TOPIC_CHARS - 1))
		t = t.substring(0, MAX_TOPIC_CHARS - 1);
	if (p.length() >= (MAX_PAYLOAD_CHARS - 1))
		p = p.substring(0, MAX_PAYLOAD_CHARS - 1);

	messageBuffer[insertIndex] = {t, p, false};
	currentIndex = insertIndex;
	resetMessageFlipState(currentIndex);

	historyActive = false;
	bumpNoIdleGuard();

	saveHistoryToPrefsNow();
}

// -------------------- Setup portal screens --------------------
void drawBootWelcomeScreen()
{
	display.clearDisplay();
	display.setTextColor(SSD1306_WHITE);

	auto centerX = [&](const char *s, uint8_t ts) -> int
	{
		int16_t x1, y1;
		uint16_t w, h;
		display.setTextSize(ts);
		display.getTextBounds(s, 0, 0, &x1, &y1, &w, &h);
		return ((OLED_WIDTH - (int)w) / 2) - x1;
	};

	display.setTextSize(2);
	display.setCursor(centerX("WELCOME", 2), 0);
	display.println("WELCOME");

	// Larger second-line text split in two lines so it remains readable on 128px width.
	display.setTextSize(2);
	display.setCursor(centerX("TO", 2), 22);
	display.println("TO");
	// Keep a single-word NOTIFICATOR line while still looking large on 128px.
	// Size 2 default spacing clips, so render per-character with tighter spacing.
	const char *notif = "NOTIFICATOR";
	const int notifLen = 11;
	const int charW = 12; // default font width at text size 2
	const int step = 11;  // tightened spacing so the full word fits
	const int totalW = charW + ((notifLen - 1) * step);
	int notifX = (OLED_WIDTH - totalW) / 2;
	int notifY = 44;

	display.setTextSize(2);
	for (int i = 0; i < notifLen; i++)
	{
		display.setCursor(notifX + (i * step), notifY);
		display.write(notif[i]);
	}

	display.display();
}

void drawSetupInstructions()
{
	static unsigned long lastDrawMs = 0;
	unsigned long now = millis();
	if (now - lastDrawMs < 250)
		return;
	lastDrawMs = now;

	display.clearDisplay();
	display.setTextColor(SSD1306_WHITE);

	display.setTextSize(2);
	display.setCursor(0, 0);
	display.println("SETUP MODE");

	display.setTextSize(1);
	display.setCursor(0, 18);
	display.println("Connect to WiFi:");

	String ssid = apSsid;
	if (!ssid.length())
		ssid = "(starting...)";

	const int maxChars = 20;
	if (ssid.length() > maxChars)
	{
		ssid = ssid.substring(0, maxChars - 3) + "...";
	}

	display.setCursor(0, 30);
	display.print("SSID: ");
	display.println(ssid);

	IPAddress ip = WiFi.softAPIP();
	display.setCursor(0, 44);
	if (ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0)
	{
		display.println("IP: starting...");
	}
	else
	{
		char ipBuf[20];
		snprintf(ipBuf, sizeof(ipBuf), "%u.%u.%u.%u", ip[0], ip[1], ip[2], ip[3]);
		display.print("IP: ");
		display.println(ipBuf);
	}

	display.setCursor(0, 56);
	display.println("Open IP in browser");

	display.display();
}

void drawPortalAnimationFrame()
{
	unsigned long now = millis();
	if (now - animLastMs < 120)
		return;
	animLastMs = now;
	animFrame++;

	display.clearDisplay();

	display.setTextSize(2);
	display.setTextColor(SSD1306_WHITE);
	display.setCursor(0, 0);
	display.println("SETUP");
	display.setTextSize(1);
	display.setCursor(0, 18);
	display.println("IN PROGRESS");

	const int cx = OLED_WIDTH - 10;
	const int cy = 10;
	const int r = 4;

	for (int i = 0; i < 8; i++)
	{
		float a = (float)i * 0.785398f;
		int x = cx + (int)(cosf(a) * r);
		int y = cy + (int)(sinf(a) * r);
		if (i == (animFrame % 8))
			display.fillCircle(x, y, 1, SSD1306_WHITE);
		else
			display.drawPixel(x, y, SSD1306_WHITE);
	}

	display.setCursor(0, 40);
	display.println("Config portal active");
	display.setCursor(0, 50);
	display.println("Saving settings...");

	display.display();
}

void startSetupPortal()
{
	drawSetupInstructions();
	setupScreenDrawn = false;
	flashOnboardLedColor(170, 60, 255, 70);

	WiFi.disconnect(true, false);
	delay(150);

	WiFi.mode(WIFI_AP_STA);
	delay(150);

	wm.setConfigPortalBlocking(false);

	portalRunning = true;
	portalStartMs = millis();
	portalStartChecked = false;

	wifiConnectingSinceMs = 0;
	lastWifiAttemptMs = 0;
	wifiHardResetDone = false;

	if (WIFI_AP_PASSWORD[0] != '\0')
		wm.startConfigPortal(apSsid.c_str(), WIFI_AP_PASSWORD);
	else
		wm.startConfigPortal(apSsid.c_str());

	bumpNoIdleGuard();
}

void finalizeSetupAfterPortal()
{
	saveConfiguredFlag(true);
	deviceConfigured = true;
	saveLastSsidForInfo(WiFi.SSID());

	drawCenteredText("SAVING", "SETTINGS", 2);

	lastMqttAttemptMs = 0;
	bumpNoIdleGuard();
	flashOnboardLedColor(0, 200, 140, 80);

	portalRunning = false;
	WiFi.mode(WIFI_STA);
	startStaConnectStable();
}

// -------------------- Device ID screen --------------------
void drawDeviceIdScreen()
{
	display.clearDisplay();
	drawStatusBar(false);

	auto centerX1 = [&](const char *s, uint8_t ts) -> int
	{
		int16_t x1, y1;
		uint16_t w, h;
		display.setTextSize(ts);
		display.getTextBounds(s, 0, 0, &x1, &y1, &w, &h);
		return ((OLED_WIDTH - (int)w) / 2) - x1;
	};

	display.setTextSize(1);
	display.setTextColor(SSD1306_WHITE);
	display.setCursor(centerX1("DEVICE ID", 1), 14);
	display.print("DEVICE ID");

	// Prioritize readability: larger ID, compact version/date metadata.
	String shortId = deviceId;
	if (shortId.length() > 8)
		shortId = shortId.substring(shortId.length() - 8);
	shortId.toUpperCase();
	display.setTextSize(2);
	display.setCursor(centerX1(shortId.c_str(), 2), 25);
	display.print(shortId);

	char fwBuf[20];
	snprintf(fwBuf, sizeof(fwBuf), "v%s", FW_VERSION);
	display.setTextSize(1);
	display.setCursor(centerX1(fwBuf, 1), 49);
	display.print(fwBuf);

	display.setTextSize(1);
	display.setCursor(centerX1(FW_VERSION_DATE, 1), 57);
	display.print(FW_VERSION_DATE);

	display.display();
}

void drawHoldCounter(unsigned long heldMs)
{
	static unsigned long lastDrawMs = 0;
	unsigned long now = millis();
	if (now - lastDrawMs < 120)
		return;
	lastDrawMs = now;

	display.clearDisplay();
	drawStatusBar(false);
	display.setTextColor(SSD1306_WHITE);

	display.setTextSize(1);
	const char *holdLabel = "HOLDING";
	int16_t x1, y1;
	uint16_t w, h;
	display.getTextBounds(holdLabel, 0, 0, &x1, &y1, &w, &h);
	display.setCursor(((OLED_WIDTH - (int)w) / 2) - x1, 16);
	display.println("HOLDING");

	unsigned long whole = heldMs / 1000;
	unsigned long tenths = (heldMs % 1000) / 100;
	char secBuf[16];
	snprintf(secBuf, sizeof(secBuf), "%lu.%lus", whole, tenths);

	display.setTextSize(3);
	display.getTextBounds(secBuf, 0, 0, &x1, &y1, &w, &h);
	display.setCursor(((OLED_WIDTH - (int)w) / 2) - x1, 24);
	display.print(secBuf);

	display.setTextSize(1);
	const char *action = (heldMs >= BUTTON_SETUP_LONG_PRESS_MS)
							 ? "RELEASE: SETUP"
							 : ((heldMs >= BUTTON_LONG_PRESS_MS)
									? "RELEASE: CLEAR"
									: "KEEP HOLDING");
	display.getTextBounds(action, 0, 0, &x1, &y1, &w, &h);
	display.setCursor(((OLED_WIDTH - (int)w) / 2) - x1, 56);
	display.print(action);

	display.display();
}

// -------------------- Weather --------------------
const char *weatherCodeToShort(uint8_t code)
{
	// Open-Meteo weathercode mapping (compact)
	switch (code)
	{
	case 0:
		return "CLR";
	case 1:
		return "MCLR";
	case 2:
		return "PCLD";
	case 3:
		return "CLD";
	case 45:
	case 48:
		return "FOG";
	case 51:
	case 53:
	case 55:
		return "DRZ";
	case 61:
	case 63:
	case 65:
		return "RAIN";
	case 66:
	case 67:
		return "FRZR";
	case 71:
	case 73:
	case 75:
		return "SNOW";
	case 77:
		return "SGRN";
	case 80:
	case 81:
	case 82:
		return "SHWR";
	case 85:
	case 86:
		return "SNSH";
	case 95:
		return "TSTM";
	case 96:
	case 99:
		return "HAIL";
	default:
		return "WX";
	}
}

bool fetchGeoByIPNow()
{
	if (!WiFi.isConnected())
		return false;

	WiFiClient client;

	HTTPClient http;
	if (!http.begin(client, "http://ip-api.com/json/"))
		return false;

	int code = http.GET();
	if (code != 200)
	{
		http.end();
		return false;
	}

	String body = http.getString();
	http.end();

	StaticJsonDocument<1536> doc;
	if (deserializeJson(doc, body) != DeserializationError::Ok)
		return false;

	const char *status = doc["status"] | "";
	if (strcmp(status, "success") != 0)
		return false;

	// ip-api.com fields: city, lat, lon, timezone
	float lat = doc["lat"] | 0.0f;
	float lon = doc["lon"] | 0.0f;
	const char *city = doc["city"] | "";
	const char *tz = doc["timezone"] | "Europe/Athens";

	if (lat == 0.0f && lon == 0.0f)
		return false;

	geoLat = lat;
	geoLon = lon;

	if (city && city[0])
	{
		geoCity = String(city);
		geoCity.toUpperCase();
		if (geoCity.length() > 12)
			geoCity = geoCity.substring(0, 12);
	}
	else
	{
		geoCity = "GEO";
	}

	geoTz = String(tz);

	geoHasData = true;
	return true;
}

void maybeFetchGeoByIP()
{
	if (!(idleTheme == 1 || idleTheme == 2))
		return;
	if (!WiFi.isConnected())
		return;
	if (geoManualOverride && geoHasData)
		return;

	unsigned long now = millis();
	if (geoFetching)
		return;

	bool due = (lastGeoFetchMs == 0) || (now - lastGeoFetchMs >= GEO_REFRESH_MS);
	if (!due && geoHasData)
		return;

	geoFetching = true;
	bool ok = fetchGeoByIPNow();
	lastGeoFetchMs = now;
	geoFetching = false;

	if (!ok)
	{
		geoHasData = false;
		geoCity = "GEO";
		return;
	}

	// Update TZ if geo fetch succeeded (ESP32 expects POSIX TZ)
	if (geoTz.length())
	{
		applyDeviceTimezone();
	}
}

bool fetchWeatherNow()
{
	if (!WiFi.isConnected())
		return false;

	WiFiClientSecure client;
	client.setInsecure(); // weather is not sensitive; keeps setup simple

	HTTPClient http;

	// Open-Meteo expects IANA timezone names; POSIX TZ strings break requests.
	String weatherTz = "auto";
	bool looksLikeIana = (geoTz.indexOf('/') >= 0) && (geoTz.indexOf(',') < 0);
	if (looksLikeIana)
	{
		weatherTz = geoTz;
	}

	// URL-encode timezone value (IANA paths include '/').
	String tzEnc = weatherTz;
	tzEnc.replace("/", "%2F");
	String url = "https://api.open-meteo.com/v1/forecast?latitude=" + String(geoLat, 4) +
				 "&longitude=" + String(geoLon, 4) +
				 "&current=temperature_2m,wind_speed_10m,weather_code" +
				 "&timezone=" + tzEnc;

	if (!http.begin(client, url))
		return false;

	int code = http.GET();
	if (code != 200)
	{
		http.end();
		return false;
	}

	String body = http.getString();
	http.end();

	StaticJsonDocument<1536> doc;
	if (deserializeJson(doc, body) != DeserializationError::Ok)
		return false;

	JsonObject cur = doc["current"];
	if (cur.isNull())
		return false;

	weatherTempC = cur["temperature_2m"] | weatherTempC;
	weatherWindKmh = cur["wind_speed_10m"] | weatherWindKmh;
	weatherCode = (uint8_t)(cur["weather_code"] | weatherCode);

	weatherHasData = true;
	return true;
}

void maybeFetchWeather()
{
	if (!(idleTheme == 1 || idleTheme == 2))
		return;
	if (!WiFi.isConnected())
		return;

	// Only trigger geo lookups when weather theme is selected.
	maybeFetchGeoByIP();
	if (!geoHasData)
		return;

	unsigned long now = millis();
	if (weatherFetching)
		return;

	bool due = (lastWeatherFetchMs == 0) || (now - lastWeatherFetchMs >= WEATHER_REFRESH_MS);
	if (!due)
		return;

	weatherFetching = true;
	bool ok = fetchWeatherNow();
	lastWeatherFetchMs = now;
	weatherFetching = false;

	(void)ok;
}

void drawIdleWeatherFrame()
{
	maybeFetchWeather();

	display.clearDisplay();
	drawStatusBar(true);

	display.setTextColor(SSD1306_WHITE);

	// main: temp
	display.setTextSize(4);
	String t = weatherHasData ? (String((int)round(weatherTempC)) + "C") : String("--C");
	int16_t x1, y1;
	uint16_t w, h;
	display.getTextBounds(t.c_str(), 0, 0, &x1, &y1, &w, &h);
	display.setCursor((OLED_WIDTH - (int)w) / 2, 16);
	display.print(t);

	// bottom: condition + wind, centered
	display.setTextSize(2);
	String bottomLine;
	if (!WiFi.isConnected())
	{
		bottomLine = "NO WIFI";
	}
	else if (geoFetching)
	{
		bottomLine = "LOCATING...";
	}
	else if (weatherFetching)
	{
		bottomLine = "FETCHING...";
	}
	else if (!weatherHasData)
	{
		bottomLine = "NO DATA";
	}
	else
	{
		bottomLine = String(weatherCodeToShort(weatherCode));
	}

	int16_t bx1, by1;
	uint16_t bw, bh;
	display.getTextBounds(bottomLine.c_str(), 0, 0, &bx1, &by1, &bw, &bh);
	display.setCursor((OLED_WIDTH - (int)bw) / 2, 48);
	display.print(bottomLine);

	display.display();
}

// -------------------- Commands --------------------
bool isValidOtaUrl(const String &url)
{
	String u = url;
	u.trim();
	if (!u.length())
		return false;

	if (OTA_REQUIRE_HTTPS)
		return u.startsWith("https://");
	return u.startsWith("https://") || u.startsWith("http://");
}

static String extractUrlHost(const String &url)
{
	int schemePos = url.indexOf("://");
	int hostStart = (schemePos >= 0) ? (schemePos + 3) : 0;
	int hostEnd = url.indexOf('/', hostStart);

	String hostPort = (hostEnd >= 0) ? url.substring(hostStart, hostEnd)
									 : url.substring(hostStart);

	int atPos = hostPort.lastIndexOf('@');
	if (atPos >= 0)
		hostPort = hostPort.substring(atPos + 1);

	int colonPos = hostPort.indexOf(':');
	String host = (colonPos >= 0) ? hostPort.substring(0, colonPos)
								  : hostPort;

	host.trim();
	host.toLowerCase();
	if (host.endsWith("."))
		host.remove(host.length() - 1);
	return host;
}

bool isAllowedOtaHost(const String &url)
{
	String host = extractUrlHost(url);
	if (!host.length())
		return false;

	// Enforce wildcard-style policy: only subdomains of wp-notificator.com.
	// Example allowed: updates.wp-notificator.com
	// Example denied: wp-notificator.com, evilwp-notificator.com
	return host.endsWith(OTA_ALLOWED_HOST_SUFFIX);
}

bool parseVersionTriplet(const String &v, int &majorV, int &minorV, int &patchV)
{
	majorV = 0;
	minorV = 0;
	patchV = 0;

	int d1 = v.indexOf('.');
	if (d1 < 0)
		return false;
	int d2 = v.indexOf('.', d1 + 1);
	if (d2 < 0)
		return false;

	String a = v.substring(0, d1);
	String b = v.substring(d1 + 1, d2);
	String c = v.substring(d2 + 1);

	a.trim();
	b.trim();
	c.trim();

	if (!a.length() || !b.length() || !c.length())
		return false;

	for (size_t i = 0; i < a.length(); i++)
		if (!isDigit(a[i]))
			return false;
	for (size_t i = 0; i < b.length(); i++)
		if (!isDigit(b[i]))
			return false;
	for (size_t i = 0; i < c.length(); i++)
		if (!isDigit(c[i]))
			return false;

	majorV = a.toInt();
	minorV = b.toInt();
	patchV = c.toInt();
	return true;
}

bool isRemoteVersionNewer(const String &currentV, const String &remoteV)
{
	int cMaj = 0, cMin = 0, cPat = 0;
	int rMaj = 0, rMin = 0, rPat = 0;

	if (!parseVersionTriplet(currentV, cMaj, cMin, cPat))
		return false;
	if (!parseVersionTriplet(remoteV, rMaj, rMin, rPat))
		return false;

	if (rMaj != cMaj)
		return rMaj > cMaj;
	if (rMin != cMin)
		return rMin > cMin;
	return rPat > cPat;
}

String buildOtaSignBase(const String &url, const String &version, unsigned long ts, bool force, const String &device)
{
	String base;
	base.reserve(url.length() + version.length() + device.length() + 32);
	base += device;
	base += "|";
	base += url;
	base += "|";
	base += version;
	base += "|";
	base += String(ts);
	base += "|";
	base += (force ? "1" : "0");
	return base;
}

bool hmacSha256Hex(const String &key, const String &message, String &outHex)
{
	outHex = "";

	const mbedtls_md_info_t *md = mbedtls_md_info_from_type(MBEDTLS_MD_SHA256);
	if (!md)
		return false;

	unsigned char mac[32];
	int rc = mbedtls_md_hmac(md,
							 (const unsigned char *)key.c_str(), key.length(),
							 (const unsigned char *)message.c_str(), message.length(),
							 mac);
	if (rc != 0)
		return false;

	static const char hex[] = "0123456789abcdef";
	char buf[65];
	for (int i = 0; i < 32; i++)
	{
		buf[i * 2] = hex[(mac[i] >> 4) & 0x0F];
		buf[i * 2 + 1] = hex[mac[i] & 0x0F];
	}
	buf[64] = '\0';
	outHex = String(buf);
	return true;
}

bool publishFirmwareTelemetry(const char *eventName, const char *otaStatus, const String &targetVersion, const String &error)
{
	if (!WiFi.isConnected())
		return false;
	if (!timeReady())
		return false;
	if (!TELEMETRY_API_URL || !TELEMETRY_API_URL[0])
		return false;
	if (!TELEMETRY_API_TOKEN || !TELEMETRY_API_TOKEN[0])
		return false;

	StaticJsonDocument<256> doc;
	doc["type"] = "device_telemetry";
	doc["event"] = (eventName && eventName[0]) ? eventName : "firmware_report";
	doc["deviceId"] = deviceId;
	doc["fwVersion"] = FW_VERSION;

	if (otaStatus && otaStatus[0])
		doc["otaStatus"] = otaStatus;
	if (targetVersion.length())
		doc["targetVersion"] = targetVersion;
	if (error.length())
		doc["error"] = error;

	String payload;
	payload.reserve(220);
	serializeJson(doc, payload);

	auto postTelemetry = [&](const char *caCert, bool insecureMode) -> int
	{
		WiFiClientSecure client;
		if (insecureMode)
		{
			client.setInsecure();
		}
		else if (caCert && caCert[0])
		{
			client.setCACert(caCert);
		}
		else
		{
			return -1;
		}

		HTTPClient http;
		if (!http.begin(client, TELEMETRY_API_URL))
		{
			return -1;
		}

		http.addHeader("Content-Type", "application/json");
		http.addHeader("x-device-telemetry-token", TELEMETRY_API_TOKEN);
		int code = http.POST(payload);
		http.end();
		return code;
	};

	int code = -1;
	if (TELEMETRY_TLS_INSECURE)
	{
		code = postTelemetry(nullptr, true);
	}
	else
	{
		// Strict TLS with pinned telemetry trust anchor.
		code = postTelemetry(TELEMETRY_CA_CERT, false);
	}

	return code >= 200 && code < 300;
}

void performOtaUpdate(const String &url, const String &targetVersion)
{
	static const unsigned long OTA_STATUS_HOLD_MS = 900;
	static const unsigned long OTA_STATUS_HOLD_LONG_MS = 1300;

	String cleanUrl = url;
	cleanUrl.trim();

	if (!isValidOtaUrl(cleanUrl))
	{
		drawCenteredText("OTA URL", "INVALID", 2);
		publishFirmwareTelemetry("ota_result", "failed", targetVersion, "invalid_url");
		flashOnboardLedColor(255, 40, 40, 120);
		delay(OTA_STATUS_HOLD_LONG_MS);
		return;
	}

	if (!isAllowedOtaHost(cleanUrl))
	{
		drawCenteredText("OTA HOST", "DENIED", 2);
		publishFirmwareTelemetry("ota_result", "failed", targetVersion, "host_denied");
		flashOnboardLedColor(255, 40, 40, 120);
		delay(OTA_STATUS_HOLD_LONG_MS);
		return;
	}

	drawCenteredText("OTA", "STARTING", 2);
	flashOnboardLedColor(60, 140, 255, 120);
	delay(OTA_STATUS_HOLD_MS);

	WiFiClientSecure client;
	if (!OTA_CA_CERT || OTA_CA_CERT[0] == '\0')
	{
		drawCenteredText("OTA TLS", "NO CERT", 2);
		publishFirmwareTelemetry("ota_result", "failed", targetVersion, "missing_ca_cert");
		flashOnboardLedColor(255, 40, 40, 120);
		delay(OTA_STATUS_HOLD_LONG_MS);
		return;
	}
	client.setCACert(OTA_CA_CERT);

	// Reboot manually only after we confirm success and show feedback.
	httpUpdate.rebootOnUpdate(false);

	httpUpdate.onStart([]()
					   { drawCenteredText("UPDATING", "FIRMWARE", 2); });

	httpUpdate.onEnd([]()
					 { drawCenteredText("OTA", "DONE", 2); });

	httpUpdate.onProgress([](int cur, int total)
						  {
    static unsigned long lastProgressDrawMs = 0;
    unsigned long now = millis();
    if (now - lastProgressDrawMs < 250) return;
    lastProgressDrawMs = now;

    int pct = 0;
    if (total > 0) pct = (cur * 100) / total;
    if (pct < 0) pct = 0;
    if (pct > 100) pct = 100;

    char pctBuf[8];
    snprintf(pctBuf, sizeof(pctBuf), "%d%%", pct);
    drawCenteredText("UPDATING", pctBuf, 2); });

	httpUpdate.onError([](int err)
					   {
    char errBuf[12];
    snprintf(errBuf, sizeof(errBuf), "ERR %d", err);
    drawCenteredText("OTA FAIL", errBuf, 2); });

	t_httpUpdate_return ret = httpUpdate.update(client, cleanUrl);

	if (ret == HTTP_UPDATE_FAILED)
	{
		int lastErr = httpUpdate.getLastError();

		char errBuf[16];
		snprintf(errBuf, sizeof(errBuf), "E%d", lastErr);
		drawCenteredText("OTA FAIL", errBuf, 2);
		publishFirmwareTelemetry("ota_result", "failed", targetVersion, String(errBuf));
		delay(OTA_STATUS_HOLD_MS);

		drawCenteredText("OTA FAIL", "TRY AGAIN", 2);
		flashOnboardLedColor(255, 40, 40, 160);
		delay(OTA_STATUS_HOLD_LONG_MS);
		return;
	}

	if (ret == HTTP_UPDATE_NO_UPDATES)
	{
		drawCenteredText("OTA", "NO UPDATE", 2);
		publishFirmwareTelemetry("ota_result", "no_update", targetVersion, "no_updates");
		flashOnboardLedColor(240, 180, 30, 120);
		delay(OTA_STATUS_HOLD_LONG_MS);
		return;
	}

	// Success path: show clear feedback, then reboot.
	drawCenteredText("OTA OK", "RESTARTING", 2);
	publishFirmwareTelemetry("ota_result", "success", targetVersion, "");
	delay(150);
	flashOnboardLedColor(0, 200, 140, 160);
	delay(OTA_STATUS_HOLD_LONG_MS);
	ESP.restart();
}

void handleCmdJson(const String &json)
{
	StaticJsonDocument<512> doc;
	if (deserializeJson(doc, json) != DeserializationError::Ok)
		return;
	if (!doc.is<JsonObject>())
		return;

	const char *cmd = doc["cmd"] | "";
	int value = doc["value"] | -1;
	const char *url = doc["url"] | "";
	const char *remoteVersion = doc["version"] | "";
	const char *sig = doc["sig"] | "";
	unsigned long ts = doc["ts"] | 0;
	bool forceOta = doc["force"] | false;

	// Supported commands:
	//  - idle_theme {value:0|1|2}
	//  - clear_msgs
	//  - mark_all_read
	//  - ota {url:"https://...", version:"x.y.z", ts:unixSeconds, sig:hmacHex, force?:bool}

	if (strcmp(cmd, "idle_theme") == 0 && (value == 0 || value == 1 || value == 2))
	{
		uint8_t normalized = (uint8_t)value;
		saveIdleTheme(normalized);
		if (normalized == 0)
			drawCenteredText("IDLE", "CLOCK", 2);
		else if (normalized == 1)
			drawCenteredText("IDLE", "HYBRID", 2);
		else
			drawCenteredText("IDLE", "WEATHER", 2);
		flashOnboardLedColor(40, 120, 255, 40);
		flashOnboardLedColor(40, 120, 255, 40);
		lastUserOrMsgMs = millis() - (IDLE_AFTER_MS + 1000);
		noIdleUntilMs = millis();
		return;
	}

	if (strcmp(cmd, "clear_msgs") == 0)
	{
		clearAllMessagesAndShowFeedback();
		return;
	}

	if (strcmp(cmd, "mark_all_read") == 0)
	{
		markAllReadAndPersist();
		drawCenteredText("MARKED", "READ", 2);
		flashOnboardLedColor(0, 180, 40, 40);
		flashOnboardLedColor(0, 180, 40, 40);
		bumpNoIdleGuard();
		return;
	}

	if (strcmp(cmd, "weather_config") == 0)
	{
		bool hasLat = !doc["lat"].isNull() || !doc["latitude"].isNull();
		bool hasLon = !doc["lon"].isNull() || !doc["longitude"].isNull();

		if (hasLat != hasLon)
		{
			drawCenteredText("WEATHER", "BAD COORD", 2);
			flashOnboardLedColor(255, 120, 0, 120);
			delay(900);
			return;
		}

		float lat = geoLat;
		float lon = geoLon;
		if (hasLat && hasLon)
		{
			lat = !doc["lat"].isNull() ? doc["lat"].as<float>() : doc["latitude"].as<float>();
			lon = !doc["lon"].isNull() ? doc["lon"].as<float>() : doc["longitude"].as<float>();

			if (lat < -90.0f || lat > 90.0f || lon < -180.0f || lon > 180.0f)
			{
				drawCenteredText("WEATHER", "BAD RANGE", 2);
				flashOnboardLedColor(255, 120, 0, 120);
				delay(900);
				return;
			}
		}

		String city = "";
		if (!doc["city"].isNull())
			city = String((const char *)doc["city"]);
		if (!city.length() && !doc["location"].isNull())
			city = String((const char *)doc["location"]);
		city.trim();

		String tz = "";
		if (!doc["timezone"].isNull())
			tz = String((const char *)doc["timezone"]);
		if (!tz.length() && !doc["tz"].isNull())
			tz = String((const char *)doc["tz"]);
		tz.trim();

		geoLat = lat;
		geoLon = lon;

		if (city.length())
		{
			geoCity = city;
			geoCity.toUpperCase();
			if (geoCity.length() > 12)
				geoCity = geoCity.substring(0, 12);
		}
		else if (!geoCity.length())
		{
			geoCity = "GEO";
		}

		if (tz.length())
		{
			geoTz = tz;
		}
		else if (!geoTz.length())
		{
			geoTz = "Europe/Athens";
		}
		applyDeviceTimezone();

		// Force NTP to rebind to the active timezone and refresh quickly.
		if (WiFi.isConnected())
		{
			configTzTime(resolveTimezonePosix(), "pool.ntp.org", "time.nist.gov");
			ntpStarted = true;
		}

		geoManualOverride = true;
		geoHasData = true;
		geoFetching = false;
		lastGeoFetchMs = millis();

		// Force immediate weather refresh using the new location.
		lastWeatherFetchMs = 0;
		weatherHasData = false;
		weatherFetching = false;

		saveWeatherConfig(true);
		return;
	}

	if (strcmp(cmd, "ota") == 0)
	{
		if (!url || !url[0])
		{
			drawCenteredText("OTA URL", "MISSING", 2);
			flashOnboardLedColor(255, 120, 0, 120);
			delay(1100);
			return;
		}

		if (!remoteVersion || !remoteVersion[0])
		{
			drawCenteredText("OTA VER", "MISSING", 2);
			flashOnboardLedColor(255, 120, 0, 120);
			delay(1100);
			return;
		}

		if (!sig || !sig[0])
		{
			drawCenteredText("OTA SIG", "MISSING", 2);
			flashOnboardLedColor(255, 40, 40, 140);
			delay(1100);
			return;
		}

		if (ts == 0)
		{
			drawCenteredText("OTA TS", "MISSING", 2);
			flashOnboardLedColor(255, 40, 40, 140);
			delay(1100);
			return;
		}

		if (!timeReady())
		{
			drawCenteredText("OTA TIME", "UNSYNC", 2);
			flashOnboardLedColor(255, 120, 0, 140);
			delay(1100);
			return;
		}

		long long nowSec = (long long)time(nullptr);
		long long tsSec = (long long)ts;
		long long skew = (nowSec > tsSec) ? (nowSec - tsSec) : (tsSec - nowSec);
		if ((unsigned long)skew > OTA_TS_MAX_SKEW_SEC)
		{
			drawCenteredText("OTA TS", "EXPIRED", 2);
			flashOnboardLedColor(255, 40, 40, 140);
			delay(1100);
			return;
		}

		if (ts <= lastAcceptedOtaTs)
		{
			drawCenteredText("OTA", "REPLAY", 2);
			flashOnboardLedColor(255, 40, 40, 140);
			delay(1100);
			return;
		}

		String expectedSig;
		String signBase = buildOtaSignBase(String(url), String(remoteVersion), ts, forceOta, deviceId);
		if (!hmacSha256Hex(String(OTA_SHARED_TOKEN), signBase, expectedSig))
		{
			drawCenteredText("OTA SIG", "ERR", 2);
			flashOnboardLedColor(255, 40, 40, 140);
			delay(1100);
			return;
		}

		String providedSig = String(sig);
		providedSig.trim();
		providedSig.toLowerCase();
		if (!providedSig.equals(expectedSig))
		{
			drawCenteredText("OTA SIG", "DENIED", 2);
			flashOnboardLedColor(255, 40, 40, 140);
			delay(1100);
			return;
		}

		bool newer = isRemoteVersionNewer(String(FW_VERSION), String(remoteVersion));
		if (!forceOta && !newer)
		{
			drawCenteredText("OTA", "NO UPDATE", 2);
			flashOnboardLedColor(240, 180, 30, 120);
			delay(1100);
			return;
		}

		lastAcceptedOtaTs = ts;

		performOtaUpdate(String(url), String(remoteVersion));
		return;
	}
}

// -------------------- MQTT --------------------
void setupMqttClient()
{
	mqttClient.setServer(MQTT_HOST, MQTT_PORT);
	mqttClient.setBufferSize(1024);

	mqttClient.setCallback([](char *topic, uint8_t *payload, unsigned int length)
						   {
    String t = String(topic ? topic : "");

    String raw;
    raw.reserve(length + 1);
    for (unsigned int i = 0; i < length; i++) raw += (char)payload[i];

    if (mqttCmdTopic.length() && t == mqttCmdTopic) {
      handleCmdJson(raw);
      return;
    }

    receivingUntilMs = millis() + RECEIVING_BADGE_MS;

    String title = "";
    String body = "";
    String type = "";
    String severity = "";

    StaticJsonDocument<1024> doc;
    DeserializationError err = deserializeJson(doc, raw);

    if (!err && doc.is<JsonObject>()) {
      JsonObject o = doc.as<JsonObject>();
      if (o["title"].is<const char*>()) title = String((const char*)o["title"]);
      if (o["body"].is<const char*>()) body = String((const char*)o["body"]);
      if (o["type"].is<const char*>()) type = String((const char*)o["type"]);
      if (o["severity"].is<const char*>()) severity = String((const char*)o["severity"]);
    } else {
      int first = raw.indexOf('|');
      if (first >= 0) {
        title = raw.substring(0, first);
        String rest = raw.substring(first + 1);
        int second = rest.indexOf('|');
        if (second >= 0) {
          body = rest.substring(0, second);
          severity = rest.substring(second + 1);
        } else {
          body = rest;
        }
      } else {
        body = raw;
      }
    }

    String payloadText;
    payloadText.reserve(160);
    if (title.length() && body.length()) payloadText = title + "|" + body;
    else if (title.length()) payloadText = title;
    else payloadText = body;
    if (!payloadText.length()) payloadText = "(empty)";

    String header = makeHeaderWithType(severity.length() ? severity : type);

    pushMessage(header, payloadText);
    showCurrentMessage(false);
    unsigned long now = millis();
    if (now - lastMsgLedFlashMs >= MSG_LED_FLASH_COOLDOWN_MS) {
      flashOnboardLedColor(0, 160, 255, 80);
      lastMsgLedFlashMs = now;
    } });

	if (MQTT_USE_TLS && MQTT_CA_CERT[0] != '\0')
	{
		tlsClient.setCACert(MQTT_CA_CERT);
	}
}

void connectToMqtt()
{
	if (mqttClient.connected())
		return;
	if (!WiFi.isConnected())
		return;

	const String clientId = String("wpnotif-") + deviceId;
	bool connected = mqttClient.connect(clientId.c_str(), MQTT_USERNAME, MQTT_PASSWORD);

	if (connected)
	{
		if (mqttSubTopic.length())
			mqttClient.subscribe(mqttSubTopic.c_str(), 1);
		if (mqttCmdTopic.length())
			mqttClient.subscribe(mqttCmdTopic.c_str(), 1);
		telemetryBootReportPending = true;
		lastTelemetryRetryMs = 0;
		flashOnboardLedColor(0, 220, 160, 70);
	}
}

// -------------------- Button handling --------------------
// Gesture recognition model:
// - Debounce each input source independently (button and TTP223).
// - Merge into a single press/release stream.
// - Resolve action on release (tap count, long hold, setup hold).
// - Keep tap evaluation delayed until TAP_WINDOW_MS expires.
static bool readButtonRawPressed()
{
	int buttonV = digitalRead(BUTTON_PIN);
	return BUTTON_ACTIVE_LOW ? (buttonV == LOW) : (buttonV == HIGH);
}

static bool readTtpRawPressed()
{
	int ttpV = digitalRead(TTP223_PIN);
	return (ttpV != (int)ttpIdleLevel);
}

void handleButton()
{
	// Debounce and merge both inputs into one gesture stream.
	unsigned long now = millis();
	bool rawButton = readButtonRawPressed();
	bool rawTtp = readTtpRawPressed();

	if (rawButton != btnRawLast)
	{
		btnRawLast = rawButton;
		btnRawChangedMs = now;
	}
	if ((now - btnRawChangedMs) >= BUTTON_DEBOUNCE_MS)
	{
		btnStable = rawButton;
	}

	if (rawTtp != ttpRawLast)
	{
		ttpRawLast = rawTtp;
		ttpRawChangedMs = now;
	}
	if ((now - ttpRawChangedMs) >= BUTTON_DEBOUNCE_MS)
	{
		ttpStable = rawTtp;
	}

	bool anyPressed = (btnStable || ttpStable);

	if (!btnDown && anyPressed)
	{
		btnDown = true;
		btnDownMs = now;
		holdPreviewActive = false;
		holdPreviewMs = 0;
		// Lock press ownership to the source(s) active at press start.
		pressStartedByBtn = (btnStable || rawButton);
		pressStartedByTtp = (ttpStable || rawTtp);
		return;
	}

	if (btnDown)
	{
		bool stillPressed = false;
		if (pressStartedByBtn)
			stillPressed = stillPressed || btnStable;
		if (pressStartedByTtp)
			stillPressed = stillPressed || ttpStable;
		if (!pressStartedByBtn && !pressStartedByTtp)
			stillPressed = anyPressed;

		if (stillPressed)
		{
			unsigned long held = now - btnDownMs;
			holdPreviewMs = held;
			holdPreviewActive = (held >= HOLD_PREVIEW_START_MS);
			// While press is active, do not evaluate pending tap timeout yet.
			return;
		}

		// Release for the same source that started the press.
		btnDown = false;
		unsigned long held = now - btnDownMs;
		holdPreviewActive = false;
		holdPreviewMs = 0;

		// Ignore very short pulses, common with capacitive sensor jitter.
		if (held < TAP_MIN_PRESS_MS)
		{
			return;
		}

		if (held >= BUTTON_SETUP_LONG_PRESS_MS)
		{
			resetTapSequence();
			if (!portalRunning)
				startSetupPortal();
			pressStartedByBtn = false;
			pressStartedByTtp = false;
			bumpNoIdleGuard();
			return;
		}

		if (held >= BUTTON_LONG_PRESS_MS)
		{
			resetTapSequence();
			clearAllMessagesAndShowFeedback();
			pressStartedByBtn = false;
			pressStartedByTtp = false;
			return;
		}

		bool thisTapOnlyTtp = (pressStartedByTtp && !pressStartedByBtn);
		if (!tapPending)
			tapOnlyTtp = thisTapOnlyTtp;
		else
			tapOnlyTtp = (tapOnlyTtp && thisTapOnlyTtp);
		tapCount++;
		tapPending = true;
		tapDeadlineMs = now + TAP_WINDOW_MS;
		// Keep the UI out of idle while a gesture sequence is being evaluated.
		bumpNoIdleGuard();
		pressStartedByBtn = false;
		pressStartedByTtp = false;
		return;
	}

	if (tapPending && (long)(now - tapDeadlineMs) >= 0)
	{
		uint8_t c = tapCount;
		tapCount = 0;
		tapPending = false;
		bool onlyTtp = tapOnlyTtp;
		tapOnlyTtp = true;

		if (onlyTtp && c >= TTP223_SETUP_TAP_COUNT)
		{
			startSetupPortal();
			bumpNoIdleGuard();
			return;
		}

		// Gesture map (normal mode):
		// 1 tap = mark read, 2 taps = next message, 3 taps = toggle read/unread,
		// 4+ taps = show device id + firmware version.
		// Note: 5+ TTP-only taps are handled above as setup trigger.
		if (c >= SHOW_ID_TAP_COUNT)
		{
			showIdSticky = true;
			showIdUntilMs = 0;
			bumpNoIdleGuard();
			return;
		}
		if (c == 1)
		{
			// If info overlay is visible, close it and allow idle immediately when there are no unread messages.
			if (showIdSticky || showIdUntilMs > now)
			{
				showIdSticky = false;
				showIdUntilMs = 0;
				if (unreadCount() == 0)
				{
					lastUserOrMsgMs = millis() - (IDLE_AFTER_MS + 1000);
					noIdleUntilMs = millis();
				}
				else
				{
					bumpNoIdleGuard();
				}
				return;
			}
			markCurrentReadAndPersist();
			return;
		}
		if (c == 2)
		{
			gotoNextMessage(false);
			return;
		}
		if (c == 3)
		{
			toggleCurrentReadStateAndPersist();
			return;
		}
	}
}

// -------------------- Idle clock (theme 0) --------------------
void drawIdleClockFrame()
{
	unsigned long now = millis();
	if (now - lastClockDrawMs < CLOCK_REDRAW_MS)
		return;
	lastClockDrawMs = now;

	display.clearDisplay();
	drawStatusBar(true);

	String hhmm = humanTimeHHMM();

	int16_t x1, y1;
	uint16_t w, h;

	display.setTextColor(SSD1306_WHITE);
	display.setTextSize(4);
	display.getTextBounds(hhmm.c_str(), 0, 0, &x1, &y1, &w, &h);
	int x = (OLED_WIDTH - (int)w) / 2;
	int y = 32 - (int)h / 2;
	if (y < 18)
		y = 18;
	display.setCursor(x, y);
	display.print(hhmm);

	display.setTextSize(2);
	if (timeReady())
	{
		time_t t = time(nullptr);
		struct tm tm;
		localtime_r(&t, &tm);
		char d[12];
		snprintf(d, sizeof(d), "%02d/%02d/%04d", tm.tm_mday, tm.tm_mon + 1, tm.tm_year + 1900);
		display.getTextBounds(d, 0, 0, &x1, &y1, &w, &h);
		display.setCursor((OLED_WIDTH - (int)w) / 2, 48);
		display.print(d);
	}
	else
	{
		const char *msg = "SYNC TIME";
		display.getTextBounds(msg, 0, 0, &x1, &y1, &w, &h);
		display.setCursor((OLED_WIDTH - (int)w) / 2, 48);
		display.print(msg);
	}

	display.display();
}

void drawIdleHybridFrame()
{
	// Alternate in 2.5s phases between clock and weather idle UIs.
	unsigned long phase = (millis() / IDLE_HYBRID_PHASE_MS) % 2;
	if (phase == 0)
		drawIdleClockFrame();
	else
		drawIdleWeatherFrame();
}

// -------------------- Setup --------------------
void setup()
{
	Serial.begin(115200);
	randomSeed((uint32_t)esp_random());

	// Default to Greece timezone (POSIX TZ rules for ESP32)
	setenv("TZ", "EET-2EEST,M3.5.0/3,M10.5.0/4", 1);
	tzset();

	setOnboardLed(false);

	pinMode(BUTTON_PIN, BUTTON_ACTIVE_LOW ? INPUT_PULLUP : INPUT);
	pinMode(TTP223_PIN, INPUT);
	delay(5);
	ttpIdleLevel = (uint8_t)digitalRead(TTP223_PIN);

	btnRawLast = readButtonRawPressed();
	btnStable = btnRawLast;
	btnRawChangedMs = millis();

	ttpRawLast = readTtpRawPressed();
	ttpStable = ttpRawLast;
	ttpRawChangedMs = millis();

	btnDown = false;
	tapCount = 0;
	tapPending = false;

	Wire.begin(I2C_SDA, I2C_SCL);
	if (!display.begin(SSD1306_SWITCHCAPVCC, OLED_ADDR))
	{
		while (true)
			delay(1000);
	}

	drawBootWelcomeScreen();
	delay(1200);

	loadConfiguredFlag();
	loadIdleTheme();
	loadWeatherConfig();

	deviceId = String((uint32_t)ESP.getEfuseMac(), HEX);
	mqttSubTopic = String("devices/") + deviceId + String("/messages");
	mqttCmdTopic = String("devices/") + deviceId + String("/cmd");
	apSsid = String(WIFI_AP_PREFIX) + deviceId;

	loadHistoryFromPrefs();
	focusFirstUnreadIfAny();

	{
		std::vector<const char *> menu = {"wifi", "info", "exit"};
		wm.setMenu(menu);
	}

	WiFi.onEvent([](WiFiEvent_t event, WiFiEventInfo_t info)
				 {
    if (event == ARDUINO_EVENT_WIFI_AP_STACONNECTED) {
      portalClientConnected = true;
      setupScreenDrawn = false;
      flashOnboardLedColor(170, 60, 255, 50);
      return;
    }
    if (event == ARDUINO_EVENT_WIFI_AP_STADISCONNECTED) { portalClientConnected = false; setupScreenDrawn = false; return; }

    if (event == ARDUINO_EVENT_WIFI_STA_GOT_IP) {
      wifiConnectedAtMs = millis();
      wifiConnectingSinceMs = 0;
      lastWifiAttemptMs = 0;
      wifiHardResetDone = false;
      flashOnboardLedColor(0, 220, 70, 70);

      if (!ntpStarted) {
        const char *tzPosix = resolveTimezonePosix();
        configTzTime(tzPosix, "pool.ntp.org", "time.nist.gov");
        ntpStarted = true;
      }
      return;
    }

    if (event == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) {
      (void)info;
      if (wifiConnectingSinceMs == 0) wifiConnectingSinceMs = millis();
      flashOnboardLedColor(255, 40, 40, 50);
      return;
    } });

	setupMqttClient();

	lastClockDrawMs = 0;

	// BOOT: if no unread, make idle eligible immediately
	bumpNoIdleGuard();
	if (unreadCount() == 0)
	{
		lastUserOrMsgMs = millis() - (IDLE_AFTER_MS + 1000);
		noIdleUntilMs = millis(); // allow immediately
	}

	if (deviceConfigured)
	{
		portalRunning = false;
		drawCenteredText("STARTING", "DEVICE", 2);
		startStaConnectStable();
	}
	else
	{
		startSetupPortal();
	}
}

// -------------------- Loop --------------------
/*
  Main runtime state machine (evaluated in this order):
  1) Always process local input and deferred history saves.
  2) If WiFi is up, refresh time placeholders and weather cache.
  3) If setup portal is active (and STA is down), keep portal UI alive and return.
  4) If portal just completed (STA connected), finalize setup and return.
  5) If configured but offline, run WiFi recovery/backoff and return.
  6) If online, maintain MQTT session.
  7) Render screen: device-id overlay > idle view eligibility > message view fallback.

  The early returns intentionally isolate each state so modes do not overlap
  (for example, portal drawing and normal message rendering).
*/
void loop()
{
	unsigned long now = millis();

	handleButton();
	maybeSaveHistory();

	if (WiFi.isConnected())
	{
		restampTimePlaceholdersIfReady();
		// geo + weather happen when idleTheme is hybrid/weather (inside maybeFetchWeather)
		maybeFetchWeather();
	}

	// Keep hold feedback visible and stable while the user is pressing.
	if (holdPreviewActive && btnDown)
	{
		drawHoldCounter(holdPreviewMs);
		return;
	}

	// ---------- Portal mode ----------
	if (portalRunning && !WiFi.isConnected())
	{
		wm.process();

		if (!portalStartChecked && (millis() - portalStartMs) > 1200)
		{
			portalStartChecked = true;
			IPAddress ip = WiFi.softAPIP();
			bool apLooksUp = (ip[0] != 0 || ip[1] != 0 || ip[2] != 0 || ip[3] != 0);
			if (!apLooksUp)
			{
				drawStatus("WiFi", "AP failed");
				startSetupPortal();
			}
		}

		if (portalClientConnected)
		{
			drawPortalAnimationFrame();
			setupScreenDrawn = false;
		}
		else
		{
			drawSetupInstructions();
			setupScreenDrawn = true;
		}
		return;
	}

	// ---------- If portal was running and STA connected ----------
	if (portalRunning && WiFi.isConnected())
	{
		portalRunning = false;
		finalizeSetupAfterPortal();
		return;
	}

	// ---------- CLEAN WiFi recovery ----------
	if (deviceConfigured && !WiFi.isConnected())
	{
		if (wifiConnectingSinceMs == 0)
			wifiConnectingSinceMs = now;

		if (now - lastWifiDrawMs >= WIFI_DRAW_MS)
		{
			lastWifiDrawMs = now;
			if ((now / 600) % 2 == 0)
				drawCenteredText("CONNECTING", "WIFI", 2);
			else
				drawCenteredText("CONNECTING", "WIFI..", 2);
		}

		if (wifiConnectedAtMs != 0 && (now - wifiConnectedAtMs) < WIFI_POST_CONNECT_GRACE_MS)
			return;

		if (now - lastWifiAttemptMs >= WIFI_RETRY_MS)
		{
			lastWifiAttemptMs = now;
			WiFi.reconnect();
		}

		if (!wifiHardResetDone && (now - wifiConnectingSinceMs) >= WIFI_HARD_RESET_AFTER_MS)
		{
			hardResetWiFiStackOnce();
		}

		if ((now - wifiConnectingSinceMs) >= WIFI_PORTAL_AFTER_MS)
		{
			startSetupPortal();
		}

		return;
	}

	// ---------- MQTT ----------
	if (!WiFi.isConnected())
		return;

	if (!mqttClient.connected())
	{
		if (now - lastMqttAttemptMs >= MQTT_RECONNECT_MS)
		{
			lastMqttAttemptMs = now;
			connectToMqtt();
		}
	}
	else
	{
		mqttClient.loop();

		// If MQTT came up before NTP, retry the initial firmware report later.
		if (telemetryBootReportPending && timeReady() && (now - lastTelemetryRetryMs >= TELEMETRY_RETRY_MS))
		{
			lastTelemetryRetryMs = now;
			if (publishFirmwareTelemetry("firmware_report", nullptr, "", ""))
			{
				telemetryBootReportPending = false;
			}
		}
	}

	// ---------- Screen refresh (NO JUMPS + IDLE FIX) ----------
	static unsigned long lastDrawMs = 0;
	if (now - lastDrawMs >= 700)
	{
		lastDrawMs = now;

		if (showIdSticky || showIdUntilMs > now)
		{
			drawDeviceIdScreen();
			return;
		}

		if (showNoMessagesUntilMs > now)
		{
			drawNoMessagesScreen();
			return;
		}

		const bool idleTimeReached = (now - lastUserOrMsgMs) > IDLE_AFTER_MS;
		const bool noUnread = (unreadCount() == 0);
		const bool idleAllowed = (long)(now - noIdleUntilMs) >= 0;

		if (idleAllowed && idleTimeReached && noUnread)
		{
			if (idleTheme == 0)
				drawIdleClockFrame();
			else if (idleTheme == 1)
				drawIdleHybridFrame();
			else if (idleTheme == 2)
				drawIdleWeatherFrame();
			else
				drawIdleClockFrame();
			return;
		}

		if (hasMessages())
		{
			bool curUnread = !messageBuffer[currentIndex].read;
			drawWrappedMessage(messageBuffer[currentIndex].topic, messageBuffer[currentIndex].payload, curUnread);
		}
		else
		{
			// no messages but not allowed to idle yet => status bar only
			display.clearDisplay();
			drawStatusBar(false);
			display.display();
		}
	}
}