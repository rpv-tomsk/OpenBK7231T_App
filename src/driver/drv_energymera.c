//#include "drv_bl0937.h"
//dummy
#include <math.h>

#include "../cmnds/cmd_public.h"
#include "../hal/hal_pins.h"
#include "../logging/logging.h"
#include "../new_cfg.h"
#include "../new_pins.h"
//#include "drv_bl_shared.h"
//#include "drv_pwrCal.h"
//#include "drv_uart.h"

#include "../cJSON/cJSON.h"
#include "../hal/hal_flashVars.h"
#include "../mqtt/new_mqtt.h"
#include "../ota/ota.h"
#include "hass.h"

//TODO: Remove dependency for NTP_IsTimeSynced
#include "drv_ntp.h" 
#include "drv_public.h"

#if PLATFORM_BL602
#include <easyflash.h>
#endif

#if PLATFORM_BEKEN

#include "BkDriverTimer.h"
#include "BkDriverGpio.h"
#include "sys_timer.h"
#include "gw_intf.h"

// HLW8012 aka BL0937

#define ELE_HW_TIME 1
#define HW_TIMER_ID 0
#elif PLATFORM_BL602

#include "../../../../../../components/hal_drv/bl602_hal/hal_gpio.h"
#include "../../../../../../components/hal_drv/bl602_hal/bl_gpio.h"

#elif PLATFORM_LN882H
#include "../../sdk/OpenLN882H/mcu/driver_ln882h/hal/hal_common.h"
#include "../../sdk/OpenLN882H/mcu/driver_ln882h/hal/hal_gpio.h"
#else

#endif

/*
typedef enum energySensor_e {
	OBK__FIRST = 0,
	OBK_VOLTAGE = OBK__FIRST, // must match order in cmd_public.h
	OBK_CURRENT,
	OBK_POWER,
	OBK_POWER_APPARENT,
	OBK_POWER_REACTIVE,
	OBK_POWER_FACTOR,
	OBK_CONSUMPTION_TOTAL,
	OBK__NUM_MEASUREMENTS = OBK_CONSUMPTION_TOTAL,
	
	// TODO OBK_CONSUMPTION_LAST_HOUR is actally "sum of consumption stats recording period"
	// and won't correspond to 'last hour' unless cmd SetupEnergyStats is enabled and configured to record one hour
	// e.g. 'SetupEnergyStats 1 60 60 0': 60 sec intervals, 60 samples
	OBK_CONSUMPTION_LAST_HOUR,
	//OBK_CONSUMPTION_STATS, // represents a variable size array of energy samples, not a sensor
	// below here are sensors that are assumed to require NTP driver
	OBK_CONSUMPTION__DAILY_FIRST, //daily consumptions are assumed to be in chronological order
	OBK_CONSUMPTION_TODAY = OBK_CONSUMPTION__DAILY_FIRST,
	OBK_CONSUMPTION_YESTERDAY,
	OBK_CONSUMPTION_2_DAYS_AGO,
	OBK_CONSUMPTION_3_DAYS_AGO,
	OBK_CONSUMPTION__DAILY_LAST = OBK_CONSUMPTION_3_DAYS_AGO,
	
	OBK_CONSUMPTION_CLEAR_DATE,
	OBK__LAST = OBK_CONSUMPTION_CLEAR_DATE,
	OBK__NUM_SENSORS,
} energySensor_t;
*/

typedef enum emSensor_e {
	EM__FIRST = 0,
	EM_POWER_L1 = EM__FIRST, // must match order in cmd_public.h ... OUCH
	EM_POWER_L2,
	EM_POWER_L3,
	EM_POWER_TOTAL,
	EM_CONSUMPTION_L1,
	EM_CONSUMPTION_L2,
	EM_CONSUMPTION_L3,
	EM_CONSUMPTION_TOTAL,
	EM__NUM_MEASUREMENTS = EM_CONSUMPTION_TOTAL,
/*	
	// TODO OBK_CONSUMPTION_LAST_HOUR is actally "sum of consumption stats recording period"
	// and won't correspond to 'last hour' unless cmd SetupEnergyStats is enabled and configured to record one hour
	// e.g. 'SetupEnergyStats 1 60 60 0': 60 sec intervals, 60 samples
	OBK_CONSUMPTION_LAST_HOUR,
	//OBK_CONSUMPTION_STATS, // represents a variable size array of energy samples, not a sensor
*/
	// below here are sensors that are assumed to require NTP driver
	EM_CONSUMPTION__DAILY_FIRST, //daily consumptions are assumed to be in chronological order
	EM_CONSUMPTION_TODAY = EM_CONSUMPTION__DAILY_FIRST,
	EM_CONSUMPTION_YESTERDAY,
	EM_CONSUMPTION_2_DAYS_AGO,
	EM_CONSUMPTION_3_DAYS_AGO,
	EM_CONSUMPTION__DAILY_LAST = EM_CONSUMPTION_3_DAYS_AGO,
	EM_CONSUMPTION_CLEAR_DATE,
	EM__LAST = OBK_CONSUMPTION_CLEAR_DATE,
	EM__NUM_SENSORS,
} emSensor_t;


static int stat_updatesSkipped = 0;
static int stat_updatesSent = 0;

// Order corrsponds to enums OBK_VOLTAGE - OBK__LAST
// note that Wh/kWh units are overridden in hass_init_energy_sensor_device_info()
static const char UNIT_WH[] = "Wh";
struct {
	energySensorNames_t names;
	byte rounding_decimals;
	// Variables below are for optimization
	// We can't send a full MQTT update every second.
	// It's too much for Beken, and it's too much for LWIP 2 MQTT library,
	// especially when actively browsing site and using JS app Log Viewer.
	// It even fails to publish with -1 error (can't alloc next packet)
	// So we publish when value changes from certain threshold or when a certain time passes.
	float changeSendThreshold;
	double *lastReading; //double only needed for energycounter i.e. OBK_CONSUMPTION_TOTAL to avoid rounding errors as value becomes high
	double lastSentValue; // what are the last values we sent over the MQTT?
	int noChangeFrame; // how much update frames has passed without sending MQTT update of read values?
} em_sensors[EM__NUM_SENSORS] = { 
	//.hass_dev_class, 	.units,		.name_friendly,			.name_mqtt,		 .hass_uniq_index, .rounding_decimals, .changeSendThreshold		
	{{"power",			"W",		"Power L1",				"power_l1",					0,		},	2,			0.25,	&readings[OBK_POWER_L1]	},	// EM_POWER_L1
	{{"power",			"W",		"Power L2",				"power_l2",					1,		},	2,			0.25,	&readings[OBK_POWER_L2]	},	// EM_POWER_L2
	{{"power",			"W",		"Power L3",				"power_l3",					2,		},	2,			0.25,	&readings[OBK_POWER_L3]	},	// EM_POWER_L3
	{{"power",			"W",		"Power Total",			"power_total",				3,		},	2,			0.25,	&readings[OBK_POWER]	},	// EM_POWER_TOTAL
	{{"energy",			UNIT_WH,	"Energy L1",			"energycounter_l1",			4,		},	3,			0.1,	&readings[OBK_CONSUMPTION_L1]	},	// EM_CONSUMPTION_L1
	{{"energy",			UNIT_WH,	"Energy L2",			"energycounter_l2",			5,		},	3,			0.1,	&readings[OBK_CONSUMPTION_L2]	},	// EM_CONSUMPTION_L2
	{{"energy",			UNIT_WH,	"Energy L3",			"energycounter_l3",			6,		},	3,			0.1,	&readings[OBK_CONSUMPTION_L3]	},	// EM_CONSUMPTION_L3
	{{"energy",			UNIT_WH,	"Energy Total",			"energycounter_total",		7,		},	3,			0.1,	&readings[OBK_CONSUMPTION_TOTAL]	},	// EM_CONSUMPTION_TOTAL
/*
	{{"energy",			UNIT_WH,	"Energy Last Hour",		"energycounter_last_hour",	4,		},	3,			0.1,		},	// OBK_CONSUMPTION_LAST_HOUR
	//{{"",				"",			"Consumption Stats",	"consumption_stats",		5,		},	0,			0,			},	// OBK_CONSUMPTION_STATS
*/
	{{"energy",			UNIT_WH,	"Energy Today",			"energycounter_today",		9,		},	3,			0.1,	&readings[OBK_CONSUMPTION_TODAY]	},	// EM_CONSUMPTION_TODAY
	{{"energy",			UNIT_WH,	"Energy Yesterday",		"energycounter_yesterday",	10,		},	3,			0.1,	&readings[OBK_CONSUMPTION_YESTERDAY]	},	// EM_CONSUMPTION_YESTERDAY
	{{"energy",			UNIT_WH,	"Energy 2 Days Ago",	"energycounter_2_days_ago",	12,		},	3,			0.1,	&readings[OBK_CONSUMPTION_2_DAYS_AGO]	},	// EM_CONSUMPTION_2_DAYS_AGO
	{{"energy",			UNIT_WH,	"Energy 3 Days Ago",	"energycounter_3_days_ago",	13,		},	3,			0.1,	&readings[OBK_CONSUMPTION_3_DAYS_AGO]	},	// EM_CONSUMPTION_3_DAYS_AGO
	{{"timestamp",		"",			"Energy Clear Date",	"energycounter_clear_date",	8,		},	0,			86400,	&readings[OBK_CONSUMPTION_CLEAR_DATE]	},	// EM_CONSUMPTION_CLEAR_DATE	
}; 

void ENERGYMERA_SaveEmeteringStatistics();
static void ENERGYMERA_GetEmeteringStatistics();
static void ENERGYMERA_ProcessUpdate(uint32_t res_L1, uint32_t res_L2, uint32_t res_L3);

bool EMERA_PublishHASSDevices(const char *topic) {
	HassDeviceInfo* dev_info;
	bool discoveryQueued = false;
	
	for (int index = EM__FIRST; index < EM__LAST; index++)
	{
		//if (index > OBK__LAST) return discoveryQueued;
		//if (index >= OBK_CONSUMPTION__DAILY_FIRST && !DRV_IsRunning("NTP")) return discoveryQueued; //include daily stats only when time is valid
		dev_info = hass_init_energy_sensor_device_info(&em_sensors[index].names);
		if (dev_info) {
			MQTT_QueuePublish(topic, dev_info->channel, hass_build_discovery_json(dev_info), OBK_PUBLISH_FLAG_RETAIN);
			hass_free_device_info(dev_info);
			discoveryQueued = true;
		}
	}
	
	return discoveryQueued;
}

#define DEFAULT_POWER_CAL 1.5f
//The above three actually are pin indices. For W600 the actual gpio_pins are different.
int GPIO_HLW_L1 = 1;
int GPIO_HLW_L2 = 2;
int GPIO_HLW_L3 = 4;
int GPIO_HLW_CAL = 3;

//float BL0937_PMAX = 3680.0f;
volatile uint32_t g_L1_pulses = 0;
volatile uint32_t g_L2_pulses = 0;
volatile uint32_t g_L3_pulses = 0;
volatile uint32_t g_CAL_pulses = 0;
uint32_t prev_L1_pulses = 0;
uint32_t prev_L2_pulses = 0;
uint32_t prev_L3_pulses = 0;
uint32_t g_CAL_end = 0;
static portTickType energyCounterStamp;
static int demoMode = 0;

#if PLATFORM_BL602
static void HlwL1Interrupt(void* arg) {
	g_L1_pulses++;
	bl_gpio_intmask(GPIO_HLW_L1, 0);
}
static void HlwL2Interrupt(void* arg) {
	g_L2_pulses++;
	bl_gpio_intmask(GPIO_HLW_L2, 0);
}
static void HlwL3Interrupt(void* arg) {
	g_L3_pulses++;
	bl_gpio_intmask(GPIO_HLW_L3, 0);
}
static void HlwCALInterrupt(void* arg) {
	g_CAL_pulses++;
	bl_gpio_intmask(GPIO_HLW_CAL, 0);

	if (g_CAL_pulses == 1) {
		g_L1_pulses = 0;
		g_L2_pulses = 0;
		g_L3_pulses = 0;
	}
	else if (g_CAL_pulses == g_CAL_end) {
		g_CAL_pulses = 0;
		CHANNEL_Set(1,0,0);
		bl_gpio_intmask(GPIO_HLW_CAL, 1);
	}
}
#endif

//static bool energyCounterStatsEnable = false;
//static int energyCounterSampleCount = 60;
//static int energyCounterSampleInterval = 60;
//static float *energyCounterMinutes = NULL;
//static portTickType energyCounterMinutesStamp;
//static long energyCounterMinutesIndex;
//static bool energyCounterStatsJSONEnable = false;

static int actual_mday = -1;
static float lastConsumptionSaveValue = 0.0f;
static portTickType lastConsumptionSaveStamp;
static float consumptionSaveThreshold = 10000.0f; //unit is Wh

static long ConsumptionSaveCounter = 0;
static time_t ConsumptionResetTime = 0;

static int changeSendAlwaysFrames = 60;
static int changeDoNotSendMinFrames = 5;

void ENERGYMERA_AppendInformationToHTTPIndexPage(http_request_t *request) {
    int i;
    const char *mode;

	mode = "ENERGYMERA";

    poststr(request, "<hr><table style='width:100%'>");

	for (i = EM__FIRST; i <= EM_CONSUMPTION__DAILY_LAST; i++) {
		if (i <= EM__NUM_MEASUREMENTS || NTP_IsTimeSynced()) {
			poststr(request, "<tr><td><b>");
			poststr(request, em_sensors[i].names.name_friendly);
			poststr(request, "</b></td><td style='text-align: right;'>");
			hprintf255(request, "%.*f</td><td>%s</td>", em_sensors[i].rounding_decimals, 
					(em_sensors[i].names.units == UNIT_WH ? 0.001 : 1) * *em_sensors[i].lastReading, //always display Energy in kwh
					 em_sensors[i].names.units == UNIT_WH ? "kWh": em_sensors[i].names.units);
#if 0
					(i == EM_CONSUMPTION_TOTAL ? 0.001 : 1) * *em_sensors[i].lastReading, //always display EM_CONSUMPTION_TOTAL in kwh
					i == EM_CONSUMPTION_TOTAL ? "kWh": em_sensors[i].names.units);
#endif
		}
		if (i == EM_CONSUMPTION_TOTAL) {
			poststr(request, "<tr><td colspan=3><hr></td></tr>");
		}
	};

    poststr(request, "</table>");

    hprintf255(request, "(changes sent %i, skipped %i, saved %li) - %s<hr>",
               stat_updatesSent, stat_updatesSkipped, ConsumptionSaveCounter,
               mode);

	poststr(request, "<h5>Energy Clear Date: ");
	if (ConsumptionResetTime) {
		struct tm *ltm = gmtime(&ConsumptionResetTime);
		hprintf255(request, "%04d-%02d-%02d %02d:%02d:%02d",
					ltm->tm_year+1900, ltm->tm_mon+1, ltm->tm_mday, ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
	} else {
		poststr(request, "(not set)");
	}
	hprintf255(request, "<br>Last Flash Saved Value: ");
	hprintf255(request, "%.3f kWh", lastConsumptionSaveValue * 0.001);

	hprintf255(request, "<br>");
	if(DRV_IsRunning("NTP")==false) {
		hprintf255(request,"NTP driver is not started, daily energy stats disbled.");
	} else if (!NTP_IsTimeSynced()) {
		hprintf255(request,"Daily energy stats awaiting NTP driver to sync real time...");
	}
	hprintf255(request, "</h5>");

#if 0
    if (energyCounterStatsEnable == true)
    {
        /********************************************************************************************************************/
        hprintf255(request,"<h2>Periodic Statistics</h2><h5>Consumption (during this period): ");
        hprintf255(request,"%1.*f Wh<br>", em_sensors[EM_CONSUMPTION_LAST_HOUR].rounding_decimals, DRV_GetReading(EM_CONSUMPTION_LAST_HOUR));
        hprintf255(request,"Sampling interval: %d sec<br>History length: ",energyCounterSampleInterval);
        hprintf255(request,"%d samples<br>History per samples:<br>",energyCounterSampleCount);
        if (energyCounterMinutes != NULL)
        {
            for(i=0; i<energyCounterSampleCount; i++)
            {
                if ((i%20)==0)
                {
                    hprintf255(request, "%1.1f", energyCounterMinutes[i]);
                } else {
                    hprintf255(request, ", %1.1f", energyCounterMinutes[i]);
                }
                if ((i%20)==19)
                {
                    hprintf255(request, "<br>");
                }
            }
			// energyCounterMinutesIndex is a long type, we need to use %ld instead of %d
            if ((i%20)!=0)
                hprintf255(request, "<br>");
            hprintf255(request, "History Index: %ld<br>JSON Stats: %s <br>", energyCounterMinutesIndex,
                    (energyCounterStatsJSONEnable == true) ? "enabled" : "disabled");
        }

        hprintf255(request, "</h5>");
    } else {
        hprintf255(request,"<h5>Periodic Statistics disabled. Use startup command SetupEnergyStats to enable function.</h5>");
    }
#endif
    /********************************************************************************************************************/
}

/*
commandResult_t BL0937_PowerMax(const void *context, const char *cmd, const char *args, int cmdFlags) {
    float maxPower;

    if(args==0||*args==0) {
        addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER,"This command needs one argument");
        return CMD_RES_NOT_ENOUGH_ARGUMENTS;
    }
    maxPower = atof(args);
    if ((maxPower>200.0) && (maxPower<7200.0f))
    {
        BL0937_PMAX = maxPower;
        // UPDATE: now they are automatically saved
        CFG_SetPowerMeasurementCalibrationFloat(CFG_OBK_POWER_MAX, BL0937_PMAX);           
        {
            char dbg[128];
            snprintf(dbg, sizeof(dbg),"PowerMax: set max to %f\n", BL0937_PMAX);
            addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER,dbg);
        }
    }
    return CMD_RES_OK;
}
*/

static commandResult_t EMERA_ResetEnergyCounter(const void *context, const char *cmd, const char *args, int cmdFlags)
{
    if(args==0||*args==0) {
        *em_sensors[EM_CONSUMPTION_TOTAL].lastReading = 0.0;
#if 0
        if (energyCounterStatsEnable == true)
        {
            if (energyCounterMinutes != NULL)
            {
                for(i = 0; i < energyCounterSampleCount; i++)
                {
                    energyCounterMinutes[i] = 0.0;
                }
            }
            energyCounterMinutesStamp = xTaskGetTickCount();
            energyCounterMinutesIndex = 0;
        }
#endif
#if 0
        for(int i = EM_CONSUMPTION__DAILY_FIRST; i <= EM_CONSUMPTION__DAILY_LAST; i++)
        {
            *sensors[i].lastReading = 0.0;
        }
#endif
    } else {
        float value = atof(args);
        *em_sensors[EM_CONSUMPTION_TOTAL].lastReading = value * 1000;
    }
    ConsumptionResetTime = (time_t)NTP_GetCurrentTime();
	
	g_L1_pulses = 0;
	g_L2_pulses = 0;
	g_L3_pulses = 0;
	
	prev_L1_pulses = 0;
	prev_L2_pulses = 0;
	prev_L3_pulses = 0;

	ENERGYMERA_SaveEmeteringStatistics();
    return CMD_RES_OK;
}


#if 0
static void ENERGYMERA_Shutdown_Pins()
{
#if PLATFORM_BL602
	//There is common IRQ for all GPIO_INTs
	//SDK has no gpio_unregister_handler(), so once registered it persists until reboot
	//All we can to do is mask.
	bl_gpio_intmask(GPIO_HLW_L1, 1);
	bl_gpio_intmask(GPIO_HLW_L2, 1);
	bl_gpio_intmask(GPIO_HLW_L3, 1);
#endif
}
#endif

static void ENERGYMERA_Init_Pins() {
	int tmp;

	// if not found, this will return the already set value
	// TODO: (rpv)
#if 0
	GPIO_HLW_L1 = PIN_FindPinIndexForRole(IOR_BL0937_SEL, GPIO_HLW_L1);
	GPIO_HLW_L2 = PIN_FindPinIndexForRole(IOR_BL0937_CF,  GPIO_HLW_L2);
	GPIO_HLW_L3 = PIN_FindPinIndexForRole(IOR_BL0937_CF1, GPIO_HLW_L3);
#endif
	//BL0937_PMAX = CFG_GetPowerMeasurementCalibrationFloat(CFG_OBK_POWER_MAX, BL0937_PMAX);

	HAL_PIN_Setup_Input_Pullup(GPIO_HLW_L1);
	HAL_PIN_Setup_Input_Pullup(GPIO_HLW_L2);
	HAL_PIN_Setup_Input_Pullup(GPIO_HLW_L3);

#if PLATFORM_BL602
	tmp = hal_gpio_register_handler(HlwL1Interrupt, GPIO_HLW_L1, GPIO_INT_CONTROL_ASYNC, GPIO_INT_TRIG_NEG_PULSE, (void*) NULL);
	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "Registering L1 handler status: %i \n", tmp);
	tmp = hal_gpio_register_handler(HlwL2Interrupt, GPIO_HLW_L2, GPIO_INT_CONTROL_ASYNC, GPIO_INT_TRIG_NEG_PULSE, (void*) NULL);
	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "Registering L2 handler status: %i \n", tmp);
	tmp = hal_gpio_register_handler(HlwL3Interrupt, GPIO_HLW_L3, GPIO_INT_CONTROL_ASYNC, GPIO_INT_TRIG_NEG_PULSE, (void*) NULL);
	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "Registering L3 handler status: %i \n", tmp);
#endif

	g_L1_pulses = 0;
	g_L2_pulses = 0;
	g_L3_pulses = 0;
	
	prev_L1_pulses = 0;
	prev_L2_pulses = 0;
	prev_L3_pulses = 0;
	
	energyCounterStamp = xTaskGetTickCount();
}

typedef enum {
    PWR_CAL_MULTIPLY,
    PWR_CAL_DIVIDE
} pwr_cal_type_t;

static const pwr_cal_type_t cal_type = PWR_CAL_MULTIPLY;
static float power_cal_l1,power_cal_l2,power_cal_l3;
static int latest_raw_power;

#if 0
static commandResult_t Calibrate(const char *cmd, const char *args, float raw,
                                 float *cal, int cfg_index) {
    Tokenizer_TokenizeString(args, 0);
    if (Tokenizer_CheckArgsCountAndPrintWarning(cmd, 1)) {
        return CMD_RES_NOT_ENOUGH_ARGUMENTS;
    }

    float real = Tokenizer_GetArgFloat(0);
	if (real == 0.0f) {
        ADDLOG_ERROR(LOG_FEATURE_ENERGYMETER, "%s",
                     CMD_GetResultString(CMD_RES_BAD_ARGUMENT));
        return CMD_RES_BAD_ARGUMENT;
    }

    *cal = (cal_type == PWR_CAL_MULTIPLY ? real / raw : raw / real);
    CFG_SetPowerMeasurementCalibrationFloat(cfg_index, *cal);

    ADDLOG_INFO(LOG_FEATURE_ENERGYMETER, "%s: you gave %f, set ref to %f\n",
                cmd, real, *cal);
    return CMD_RES_OK;
}

static commandResult_t CalibratePower(const void *context, const char *cmd,
                                      const char *args, int cmdFlags) {
    return Calibrate(cmd, args, latest_raw_power, &power_cal, CFG_OBK_POWER);
}
#endif

commandResult_t EMERA_StartDemo(const void *context, const char *cmd, const char *args, int cmdFlags)
{
    if(args==0||*args==0) {
		demoMode = 1;
    } else {
        demoMode = atoi(args);
    }

    ADDLOG_INFO(LOG_FEATURE_ENERGYMETER, "DemoMode switched to %d\n", demoMode);
    return CMD_RES_OK;
}

commandResult_t EMERA_Dump(const void *context, const char *cmd, const char *args, int cmdFlags)
{
	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "Phase pulses: %i, %i, %i\n", g_L1_pulses, g_L2_pulses, g_L3_pulses);
	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "Power_cal values: %.5f, %.5f, %.5f\n", power_cal_l1, power_cal_l2, power_cal_l3);
    return CMD_RES_OK;
}

commandResult_t EMERA_Calibrate(const void *context, const char *cmd, const char *args, int cmdFlags)
{
#if PLATFORM_BL602
    if(args==0||*args==0) {
        addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "This command needs one argument");
        return CMD_RES_NOT_ENOUGH_ARGUMENTS;
    }

	g_CAL_end = atoi(args);

	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER,"Started calibration for %i pulses. power_cal before: %.5f, %.5f, %.5f; Waiting for pulse.\n", g_CAL_end, power_cal_l1,power_cal_l2,power_cal_l3);
	g_CAL_end++;

	//CHANNEL_Set(1,1,0);
	HAL_PIN_Setup_Input_Pullup(GPIO_HLW_CAL);

	int tmp = hal_gpio_register_handler(HlwCALInterrupt, GPIO_HLW_CAL, GPIO_INT_CONTROL_SYNC, GPIO_INT_TRIG_NEG_PULSE, (void*) NULL);
	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "Registering CAL handler status: %i \n", tmp);

#endif
    return CMD_RES_OK;
}

commandResult_t EMERA_SetPulses(const void *context, const char *cmd, const char *args, int cmdFlags)
{
	Tokenizer_TokenizeString(args, 0);
	if (Tokenizer_CheckArgsCountAndPrintWarning(cmd, 2)) {
		return CMD_RES_NOT_ENOUGH_ARGUMENTS;
	}

	int phase = Tokenizer_GetArgInteger(0);
	if (phase < 0 || phase > 2) {
        return CMD_RES_BAD_ARGUMENT;
	}

    int cnt = Tokenizer_GetArgInteger(1);
	if (cnt < 100) {
        return CMD_RES_BAD_ARGUMENT;
	}

	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER,"power_cal before: %.5f, %.5f, %.5f\n", power_cal_l1, power_cal_l2, power_cal_l3);

	float k = 3600000.0f / cnt;

	switch (phase) {
		case 0:
		power_cal_l1 = k;
		break;
		case 1:
		power_cal_l2 = k;
		break;
		case 2:
		power_cal_l3 = k;
		break;
	}

	CFG_SetPowerMeasurementCalibrationFloat(phase, k);
	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER,"power_cal before: %.5f, %.5f, %.5f\n", power_cal_l1, power_cal_l2, power_cal_l3);
    return CMD_RES_OK;
}

enum {
	EMERA_CFG_CAL_POWER_L1 = 0,
	EMERA_CFG_CAL_POWER_L2,
	EMERA_CFG_CAL_POWER_L3,
};

static float Scale(float raw, float cal) {
    return (cal_type == PWR_CAL_MULTIPLY ? raw * cal : raw / cal);
}

void ENERGYMERA_Init(void) {
#if PLATFORM_BL602
#else
    ADDLOG_ERROR(LOG_FEATURE_DRV, "ENERGYMERA_Init: platform not supported");
    return -1;
#endif

    for(int i = EM__FIRST; i <= EM__LAST; i++) {
        em_sensors[i].noChangeFrame = 0;
        *em_sensors[i].lastReading = 0;
    }
	
#if 0 
    if (energyCounterStatsEnable == true)
    {
        if (energyCounterMinutes == NULL)
        {
            energyCounterMinutes = (float*)os_malloc(energyCounterSampleCount*sizeof(float));
        }
        if (energyCounterMinutes != NULL)
        {
            for(i = 0; i < energyCounterSampleCount; i++)
            {
                energyCounterMinutes[i] = 0.0;
            }   
        }
        energyCounterMinutesStamp = xTaskGetTickCount();
        energyCounterMinutesIndex = 0;
    }
#endif

    //Load saved state
	ENERGYMERA_GetEmeteringStatistics();

    power_cal_l1 = CFG_GetPowerMeasurementCalibrationFloat(EMERA_CFG_CAL_POWER_L1, DEFAULT_POWER_CAL);
	power_cal_l2 = CFG_GetPowerMeasurementCalibrationFloat(EMERA_CFG_CAL_POWER_L2, DEFAULT_POWER_CAL);
	power_cal_l3 = CFG_GetPowerMeasurementCalibrationFloat(EMERA_CFG_CAL_POWER_L3, DEFAULT_POWER_CAL);

	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "EMERA started. power_cal values: %.5f, %.5f, %.5f\n", power_cal_l1, power_cal_l2, power_cal_l3);
 
	//cmddetail:{"name":"EmeraDemo","args":"",
	//cmddetail:"descr":"Start demo pulses",
	//cmddetail:"fn":"EMERA_StartDemo","file":"driver/drv_energomera.c","requires":"",
	//cmddetail:"examples":""}
    CMD_RegisterCommand("EmeraDemo", EMERA_StartDemo, NULL);

	//cmddetail:{"name":"EmeraDump","args":"",
	//cmddetail:"descr":"Dump internal data",
	//cmddetail:"fn":"EMERA_Dump","file":"driver/drv_energomera.c","requires":"",
	//cmddetail:"examples":""}	
	CMD_RegisterCommand("EmeraDump", EMERA_Dump, NULL);
	
	//cmddetail:{"name":"EMERASetPulses","args":"channel pulses_per_kwh",
	//cmddetail:"descr":"Dump internal data",
	//cmddetail:"fn":"EmeraCalibrate","file":"driver/drv_energomera.c","requires":"",
	//cmddetail:"examples":""}
	CMD_RegisterCommand("EMERASetPulses", EMERA_SetPulses, NULL);

	//cmddetail:{"name":"PowerSet","args":"Power",
	//cmddetail:"descr":"Measure the real Power with an external, reliable power meter and enter this Power via this command to calibrate. The calibration is automatically saved in the flash memory.",
	//cmddetail:"fn":"NULL);","file":"driver/drv_pwrCal.c","requires":"",
	//cmddetail:"examples":""}
    //CMD_RegisterCommand("PowerSet", CalibratePower, NULL);
	
	//cmddetail:{"name":"PowerMax","args":"BL0937_PowerMax",
	//cmddetail:"descr":"",
	//cmddetail:"fn":"NULL);","file":"driver/drv_bl0937.c","requires":"",
	//cmddetail:"examples":""}
    //CMD_RegisterCommand("PowerMax",BL0937_PowerMax, NULL);
	
	//cmddetail:{"name":"EnergyCntReset","args":"[OptionalNewValue]",
	//cmddetail:"descr":"Resets the total Energy Counter, the one that is usually kept after device reboots. After this commands, the counter will start again from 0 (or from the value you specified).",
	//cmddetail:"fn":"BL09XX_ResetEnergyCounter","file":"driver/drv_bl_shared.c","requires":"",
	//cmddetail:"examples":""}
    CMD_RegisterCommand("EnergyCntReset", EMERA_ResetEnergyCounter, NULL);

	//cmddetail:{"name":"SetupEnergyStats","args":"[Enable1or0][SampleTime][SampleCount][JSonEnable]",
	//cmddetail:"descr":"Setup Energy Statistic Parameters: [enable 0 or 1] [sample_time[10..90]] [sample_count[10..180]] [JsonEnable 0 or 1]. JSONEnable is optional.",
	//cmddetail:"fn":"BL09XX_SetupEnergyStatistic","file":"driver/drv_bl_shared.c","requires":"",
	//cmddetail:"examples":""}
    //CMD_RegisterCommand("SetupEnergyStats", BL09XX_SetupEnergyStatistic, NULL);

	//cmddetail:{"name":"ConsumptionThreshold","args":"[FloatValue]",
	//cmddetail:"descr":"Setup value for automatic save of consumption data [1..100]",
	//cmddetail:"fn":"BL09XX_SetupConsumptionThreshold","file":"driver/drv_bl_shared.c","requires":"",
	//cmddetail:"examples":""}
    //CMD_RegisterCommand("ConsumptionThreshold", BL09XX_SetupConsumptionThreshold, NULL);

	//cmddetail:{"name":"VCPPublishThreshold","args":"[VoltageDeltaVolts][CurrentDeltaAmpers][PowerDeltaWats][EnergyDeltaWh]",
	//cmddetail:"descr":"Sets the minimal change between previous reported value over MQTT and next reported value over MQTT. Very useful for BL0942, BL0937, etc. So, if you set, VCPPublishThreshold 0.5 0.001 0.5, it will only report voltage again if the delta from previous reported value is largen than 0.5V. Remember, that the device will also ALWAYS force-report values every N seconds (default 60)",
	//cmddetail:"fn":"BL09XX_VCPPublishThreshold","file":"driver/drv_bl_shared.c","requires":"",
	//cmddetail:"examples":""}
	//CMD_RegisterCommand("VCPPublishThreshold", BL09XX_VCPPublishThreshold, NULL);

	//cmddetail:{"name":"VCPPrecision","args":"[VoltageDigits][CurrentDigitsAmpers][PowerDigitsWats][EnergyDigitsWh]",
	//cmddetail:"descr":"Sets the number of digits after decimal point for power metering publishes. Default is BL09XX_VCPPrecision 1 3 2 3. This works for OBK-style publishes.",
	//cmddetail:"fn":"BL09XX_VCPPrecision","file":"driver/drv_bl_shared.c","requires":"",
	//cmddetail:"examples":""}

	//CMD_RegisterCommand("VCPPrecision", BL09XX_VCPPrecision, NULL);
	//cmddetail:{"name":"VCPPublishIntervals","args":"[MinDelayBetweenPublishes][ForcedPublishInterval]",
	//cmddetail:"descr":"First argument is minimal allowed interval in second between Voltage/Current/Power/Energy publishes (even if there is a large change), second value is an interval in which V/C/P/E is always published, even if there is no change",
	//cmddetail:"fn":"BL09XX_VCPPublishIntervals","file":"driver/drv_bl_shared.c","requires":"",
	//cmddetail:"examples":""}
	//CMD_RegisterCommand("VCPPublishIntervals", BL09XX_VCPPublishIntervals, NULL);

	ENERGYMERA_Init_Pins();
}

static float BL_ChangeEnergyUnitIfNeeded(float Wh) {
	if (CFG_HasFlag(OBK_FLAG_MQTT_ENERGY_IN_KWH)) {
		return Wh * 0.001f;
	}
	return Wh;
}

void ENERGYMERA_RunEverySecond(void) {
	// TODO: (rpv)
#if 0
	bool bNeedRestart = false;
	if (GPIO_HLW_L1 != PIN_FindPinIndexForRole(IOR_BL0937_CF, GPIO_HLW_L1)) {
		bNeedRestart = true;
	}
	if (GPIO_HLW_L2 != PIN_FindPinIndexForRole(IOR_BL0937_CF1, GPIO_HLW_L2)) {
		bNeedRestart = true;
	}
	if (GPIO_HLW_L3 != PIN_FindPinIndexForRole(IOR_BL0937_CF1, GPIO_HLW_L3)) {
		bNeedRestart = true;
	}

	if (bNeedRestart) {
		addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "ENERGYMERA pins have changed, will reset the interrupts");

		ENERGYMERA_Shutdown_Pins();
		ENERGYMERA_Init_Pins();
		return;
	}
#endif

	uint32_t res_L1 = g_L1_pulses - prev_L1_pulses;
	uint32_t res_L2 = g_L2_pulses - prev_L2_pulses;
	uint32_t res_L3 = g_L3_pulses - prev_L3_pulses;
	
	prev_L1_pulses += res_L1;
	prev_L2_pulses += res_L2;
	prev_L3_pulses += res_L3;
	
	if (demoMode) {
		res_L1 = 100;
		res_L2 = 200; 
		res_L3 = 400;
	}
	
	//TODO: disable debug (rpv)
	if (g_CAL_pulses) {
		addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER,"Phase pulses: %li, %li, %li CAL: %li\n", g_L1_pulses, g_L2_pulses, g_L3_pulses, g_CAL_pulses);
		return;
	}
	else
		addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER,"Phase pulses: %li, %li, %li\n", res_L1, res_L2, res_L3);

	ENERGYMERA_ProcessUpdate(res_L1, res_L2, res_L3);
}

void ENERGYMERA_ProcessUpdate(uint32_t res_L1, uint32_t res_L2, uint32_t res_L3) {
    int i;
    //int xPassedTicks;
    //cJSON* root;
    //cJSON* stats;
    //char *msg;
    //portTickType interval;
	portTickType ticksElapsed;
    char datetime[64];
	float diff;
	
	float energy_L1, energy_L2, energy_L3;
	float power_L1, power_L2, power_L3;
	float energy;
	
    portTickType ticks = xTaskGetTickCount();
    ticksElapsed = (ticks - energyCounterStamp);
	energyCounterStamp = ticks;
	
    latest_raw_power = res_L1; //Save for calibration

    energy_L1 = Scale(res_L1, power_cal_l1);
    energy_L2 = Scale(res_L2, power_cal_l2);
    energy_L3 = Scale(res_L3, power_cal_l3);
	
	power_L1 = energy_L1 * (1000.0f / (float)portTICK_PERIOD_MS);
	power_L1 /= (float)ticksElapsed;

	power_L2 = energy_L2 * (1000.0f / (float)portTICK_PERIOD_MS);
	power_L2 /= (float)ticksElapsed;
	
	power_L3 = energy_L3 * (1000.0f / (float)portTICK_PERIOD_MS);
	power_L3 /= (float)ticksElapsed;
	

//TODO: disable (rpv)    
#if 1
	{
		char dbg[128];
		snprintf(dbg, sizeof(dbg),"Phase power: %f, %f, %f\n", power_L1, power_L2, power_L3);
		addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, dbg);
	}
#endif

	*em_sensors[EM_POWER_L1].lastReading = power_L1;
	*em_sensors[EM_POWER_L2].lastReading = power_L2;
	*em_sensors[EM_POWER_L3].lastReading = power_L3;
	*em_sensors[EM_POWER_TOTAL].lastReading = power_L1 + power_L2 + power_L3;
	
	energy = (double)energy_L1 / (3600.0f);
	*em_sensors[EM_CONSUMPTION_L1].lastReading += energy;
	*em_sensors[EM_CONSUMPTION_TOTAL].lastReading += energy;
	*em_sensors[EM_CONSUMPTION_TODAY].lastReading += energy;

	energy = (double)energy_L2 / (3600.0f);
	*em_sensors[EM_CONSUMPTION_L2].lastReading += energy;
	*em_sensors[EM_CONSUMPTION_TOTAL].lastReading += energy;
	*em_sensors[EM_CONSUMPTION_TODAY].lastReading += energy;

	energy = (double)energy_L3 / (3600.0f);
	*em_sensors[EM_CONSUMPTION_L3].lastReading += energy;
	*em_sensors[EM_CONSUMPTION_TOTAL].lastReading += energy;	
	*em_sensors[EM_CONSUMPTION_TODAY].lastReading += energy;
	
	//TODO: rpv
    //HAL_FlashVars_SaveTotalConsumption(em_sensors[EM_CONSUMPTION_TOTAL].lastReading);

    if (NTP_IsTimeSynced()) {
        time_t ntpTime = (time_t)NTP_GetCurrentTime();
        struct tm *ltm = gmtime(&ntpTime);
		
        if (ConsumptionResetTime == 0)
            ConsumptionResetTime = (time_t)ntpTime;

        if (actual_mday == -1)
        {
            actual_mday = ltm->tm_mday;
        }
        if (actual_mday != ltm->tm_mday)
        {
            for (i = EM_CONSUMPTION__DAILY_LAST; i >= EM_CONSUMPTION__DAILY_FIRST; i--) {
                *em_sensors[i].lastReading = *em_sensors[i - 1].lastReading;
			}
            *em_sensors[EM_CONSUMPTION_TODAY].lastReading = 0.0;
            actual_mday = ltm->tm_mday;

            MQTT_PublishMain_StringFloat(em_sensors[EM_CONSUMPTION_YESTERDAY].names.name_mqtt, BL_ChangeEnergyUnitIfNeeded(*em_sensors[EM_CONSUMPTION_YESTERDAY].lastReading ),
										em_sensors[EM_CONSUMPTION_YESTERDAY].rounding_decimals, 0);
            stat_updatesSent++;
			
			ENERGYMERA_SaveEmeteringStatistics();
        }
    }

#if 0
    if (energyCounterStatsEnable == true)
    {
        interval = energyCounterSampleInterval;
        interval *= (1000 / portTICK_PERIOD_MS); 
        if ((xTaskGetTickCount() - energyCounterMinutesStamp) >= interval)
        {
			if (energyCounterMinutes != NULL) {
				*em_sensors[EM_CONSUMPTION_LAST_HOUR].lastReading = 0;
				for(int i = 0; i < energyCounterSampleCount; i++) {
					*em_sensors[EM_CONSUMPTION_LAST_HOUR].lastReading  += energyCounterMinutes[i];
				}
			}
            if ((energyCounterStatsJSONEnable == true) && (MQTT_IsReady() == true))
            {
                root = cJSON_CreateObject();
                cJSON_AddNumberToObject(root, "uptime", g_secondsElapsed);
                cJSON_AddNumberToObject(root, "consumption_total", BL_ChangeEnergyUnitIfNeeded(DRV_GetReading(EM_CONSUMPTION_TOTAL)));
                cJSON_AddNumberToObject(root, "consumption_last_hour", BL_ChangeEnergyUnitIfNeeded(DRV_GetReading(EM_CONSUMPTION_LAST_HOUR)));
                cJSON_AddNumberToObject(root, "consumption_stat_index", energyCounterMinutesIndex);
                cJSON_AddNumberToObject(root, "consumption_sample_count", energyCounterSampleCount);
                cJSON_AddNumberToObject(root, "consumption_sampling_period", energyCounterSampleInterval);
                if(NTP_IsTimeSynced() == true)
                {
                    cJSON_AddNumberToObject(root, "consumption_today", BL_ChangeEnergyUnitIfNeeded(DRV_GetReading(EM_CONSUMPTION_TODAY)));
                    cJSON_AddNumberToObject(root, "consumption_yesterday", BL_ChangeEnergyUnitIfNeeded(DRV_GetReading(EM_CONSUMPTION_YESTERDAY)));
                    ltm = gmtime(&ConsumptionResetTime);
                    if (NTP_GetTimesZoneOfsSeconds()>0)
                    {
                       snprintf(datetime,sizeof(datetime), "%04i-%02i-%02iT%02i:%02i+%02i:%02i",
                               ltm->tm_year+1900, ltm->tm_mon+1, ltm->tm_mday, ltm->tm_hour, ltm->tm_min,
                               NTP_GetTimesZoneOfsSeconds()/3600, (NTP_GetTimesZoneOfsSeconds()/60) % 60);
                    } else {
                       snprintf(datetime, sizeof(datetime), "%04i-%02i-%02iT%02i:%02i-%02i:%02i",
                               ltm->tm_year+1900, ltm->tm_mon+1, ltm->tm_mday, ltm->tm_hour, ltm->tm_min,
                               abs(NTP_GetTimesZoneOfsSeconds()/3600), (abs(NTP_GetTimesZoneOfsSeconds())/60) % 60);
                    }
                    cJSON_AddStringToObject(root, "consumption_clear_date", datetime);
                }

                if (energyCounterMinutes != NULL)
                {
                    stats = cJSON_CreateArray();
					// WARNING - it causes HA problems?
					// See: https://github.com/openshwprojects/OpenBK7231T_App/issues/870
					// Basically HA has 256 chars state limit?
					// Wait, no, it's over 256 even without samples?
                    for(i = 0; i < energyCounterSampleCount; i++)
                    {
                        cJSON_AddItemToArray(stats, cJSON_CreateNumber(energyCounterMinutes[i]));
                    }
                    cJSON_AddItemToObject(root, "consumption_samples", stats);
                }

                if(NTP_IsTimeSynced() == true)
                {
                    stats = cJSON_CreateArray();
                    for(i = EM_CONSUMPTION__DAILY_FIRST; i <= EM_CONSUMPTION__DAILY_LAST; i++)
                    {
                        cJSON_AddItemToArray(stats, cJSON_CreateNumber(DRV_GetReading(i)));
                    }
                    cJSON_AddItemToObject(root, "consumption_daily", stats);
                }

                msg = cJSON_PrintUnformatted(root);
                cJSON_Delete(root);

               // addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "JSON Printed: %d bytes", strlen(msg));

                MQTT_PublishMain_StringString("consumption_stats", msg, 0);
                stat_updatesSent++;
                os_free(msg);
            }

            if (energyCounterMinutes != NULL)
            {
                for (i=energyCounterSampleCount-1;i>0;i--)
                {
                    if (energyCounterMinutes[i-1]>0.0)
                    {
                        energyCounterMinutes[i] = energyCounterMinutes[i-1];
                    } else {
                        energyCounterMinutes[i] = 0.0;
                    }
                }
                energyCounterMinutes[0] = 0.0;
            }
            energyCounterMinutesStamp = xTaskGetTickCount();
            energyCounterMinutesIndex++;
        }

        if (energyCounterMinutes != NULL)
            energyCounterMinutes[0] += energy;
    }
#endif 

    for(i = EM__FIRST; i <= EM__LAST; i++)
    {
        // send update only if there was a big change or if certain time has passed
        // Do not send message with every measurement. 
		diff = em_sensors[i].lastSentValue - *em_sensors[i].lastReading;
		// check for change
        if ( ((fabsf(diff) > em_sensors[i].changeSendThreshold) &&
               (em_sensors[i].noChangeFrame >= changeDoNotSendMinFrames)) ||
             (em_sensors[i].noChangeFrame >= changeSendAlwaysFrames) )
        {
            em_sensors[i].noChangeFrame = 0;
/*
			enum EventCode eventChangeCode;
			switch (i) {
				case OBK_VOLTAGE:				eventChangeCode = CMD_EVENT_CHANGE_VOLTAGE;	break;
				case OBK_CURRENT:				eventChangeCode = CMD_EVENT_CHANGE_CURRENT;	break;
				case OBK_POWER:					eventChangeCode = CMD_EVENT_CHANGE_POWER; break;
				case OBK_CONSUMPTION_TOTAL:		eventChangeCode = CMD_EVENT_CHANGE_CONSUMPTION_TOTAL; break;
				case OBK_CONSUMPTION_LAST_HOUR:	eventChangeCode = CMD_EVENT_CHANGE_CONSUMPTION_LAST_HOUR; break;
				default:						eventChangeCode = CMD_EVENT_NONE; break;
			}

			switch (eventChangeCode) {
				case CMD_EVENT_NONE:
					break;
				case CMD_EVENT_CHANGE_CURRENT: ;
					int prev_mA = em_sensors[i].lastSentValue * 1000;
					int now_mA = *em_sensors[i].lastReading * 1000;
					EventHandlers_ProcessVariableChange_Integer(eventChangeCode, prev_mA, now_mA);
					break;
				default:
					EventHandlers_ProcessVariableChange_Integer(eventChangeCode, em_sensors[i].lastSentValue, *em_sensors[i].lastReading);
					break;
			}
*/
            if (MQTT_IsReady() == true)
            {
				em_sensors[i].lastSentValue = *em_sensors[i].lastReading;
				if (i == EM_CONSUMPTION_CLEAR_DATE) {
					*em_sensors[i].lastReading = ConsumptionResetTime; //Only to make the 'nochangeframe' mechanism work here
					struct tm *ltm = gmtime(&ConsumptionResetTime);

					/* 2019-09-07T15:50-04:00 */
					if (NTP_GetTimesZoneOfsSeconds()>0)
					{
						snprintf(datetime, sizeof(datetime), "%04i-%02i-%02iT%02i:%02i+%02i:%02i",
								ltm->tm_year+1900, ltm->tm_mon+1, ltm->tm_mday, ltm->tm_hour, ltm->tm_min,
								NTP_GetTimesZoneOfsSeconds()/3600, (NTP_GetTimesZoneOfsSeconds()/60) % 60);
					} else {
						snprintf(datetime, sizeof(datetime), "%04i-%02i-%02iT%02i:%02i-%02i:%02i",
								ltm->tm_year+1900, ltm->tm_mon+1, ltm->tm_mday, ltm->tm_hour, ltm->tm_min,
								abs(NTP_GetTimesZoneOfsSeconds()/3600), (abs(NTP_GetTimesZoneOfsSeconds())/60) % 60);
					}
					MQTT_PublishMain_StringString(em_sensors[i].names.name_mqtt, datetime, 0);
				} else { //all other sensors
					float val = *em_sensors[i].lastReading;
					if (em_sensors[i].names.units == UNIT_WH) val = BL_ChangeEnergyUnitIfNeeded(val);
					MQTT_PublishMain_StringFloat(em_sensors[i].names.name_mqtt, val, em_sensors[i].rounding_decimals, 0);
				}
				stat_updatesSent++;
			}
        } else {
            // no change frame
            em_sensors[i].noChangeFrame++;
            stat_updatesSkipped++;
        }
    }        
	
	//Save threshold:
	// -- Each $consumptionSaveThreshold kWh, but no more than once an 30 minutes,
	// -- Each 6 hours
    if ((((*em_sensors[EM_CONSUMPTION_TOTAL].lastReading - lastConsumptionSaveValue) >= consumptionSaveThreshold) && 
	     ((ticks - lastConsumptionSaveStamp) >= (0.5 * 3600 * 1000 / portTICK_PERIOD_MS)))
        || ((ticks - lastConsumptionSaveStamp) >= (6 * 3600 * 1000 / portTICK_PERIOD_MS)))
    {
		ENERGYMERA_SaveEmeteringStatistics();
    }
}

#define EASYFLASH_EMERA     "emera"
/* Fixed size */
typedef struct EMERA_METERING_DATA {
	//float  - 4
	//long   - 4
	//time_t - 8
	float ConsumptionL1;
	float ConsumptionL2;
	float ConsumptionL3;
	float TotalConsumption;
	long save_counter;
	float TodayConsumpion;
	float YesterdayConsumption;
	float ConsumptionHistory[2];
	time_t ConsumptionResetTime;
	char actual_mday;
	//unsigned char reserved[3];
} EMERA_METERING_DATA;

static void ENERGYMERA_GetEmeteringStatistics() {
	EMERA_METERING_DATA data;
	
	int dataLen = sizeof(data);
	
	ADDLOG_DEBUG(LOG_FEATURE_CFG, "ENERGYMERA_GetEmeteringStatistics: will read %d bytes", dataLen);
	int readLen = ef_get_env_blob(EASYFLASH_EMERA, &data, dataLen , NULL);
	ADDLOG_DEBUG(LOG_FEATURE_CFG, "ENERGYMERA_GetEmeteringStatistics: really loaded %d bytes", readLen);
	
	if (readLen != dataLen) {
		addLogAdv(LOG_ERROR, LOG_FEATURE_ENERGYMETER, "Failed to load Emetering: got %i, expected %i\n", readLen, dataLen);
		return;
	}
	
	*em_sensors[EM_CONSUMPTION_L1].lastReading = data.ConsumptionL1;
	*em_sensors[EM_CONSUMPTION_L2].lastReading = data.ConsumptionL2;
	*em_sensors[EM_CONSUMPTION_L3].lastReading = data.ConsumptionL3;
    *em_sensors[EM_CONSUMPTION_TOTAL].lastReading = data.TotalConsumption;
	
	
    *em_sensors[EM_CONSUMPTION_TODAY].lastReading = data.TodayConsumpion;
    *em_sensors[EM_CONSUMPTION_YESTERDAY].lastReading = data.YesterdayConsumption;
    *em_sensors[EM_CONSUMPTION_2_DAYS_AGO].lastReading = data.ConsumptionHistory[0];
    *em_sensors[EM_CONSUMPTION_3_DAYS_AGO].lastReading = data.ConsumptionHistory[1];
	actual_mday = data.actual_mday;    

	lastConsumptionSaveValue = data.TotalConsumption;
	lastConsumptionSaveStamp = xTaskGetTickCount();
	
	ConsumptionResetTime = data.ConsumptionResetTime;
    ConsumptionSaveCounter = data.save_counter;
}

void ENERGYMERA_SaveEmeteringStatistics() {
    EMERA_METERING_DATA data;
	
#if WINDOWS
#elif PLATFORM_BL602
#elif PLATFORM_W600 || PLATFORM_W800
#elif PLATFORM_XR809
#elif PLATFORM_BK7231N || PLATFORM_BK7231T
        if (ota_progress() != -1)
			return;
#endif

    memset(&data, 0, sizeof(EMERA_METERING_DATA));
	
	data.ConsumptionL1 = *em_sensors[EM_CONSUMPTION_L1].lastReading;
	data.ConsumptionL2 = *em_sensors[EM_CONSUMPTION_L2].lastReading;
	data.ConsumptionL3 = *em_sensors[EM_CONSUMPTION_L3].lastReading;
	data.TotalConsumption = *em_sensors[EM_CONSUMPTION_TOTAL].lastReading;

    data.TodayConsumpion = *em_sensors[EM_CONSUMPTION_TODAY].lastReading;
    data.YesterdayConsumption = *em_sensors[EM_CONSUMPTION_YESTERDAY].lastReading;
    data.ConsumptionHistory[0] = *em_sensors[EM_CONSUMPTION_2_DAYS_AGO].lastReading;
    data.ConsumptionHistory[1] = *em_sensors[EM_CONSUMPTION_3_DAYS_AGO].lastReading;
	data.actual_mday = actual_mday;

    ConsumptionSaveCounter++;
    data.save_counter = ConsumptionSaveCounter;
	data.ConsumptionResetTime = ConsumptionResetTime;
	
	lastConsumptionSaveValue = *em_sensors[EM_CONSUMPTION_TOTAL].lastReading;
	lastConsumptionSaveStamp = xTaskGetTickCount();

    //HAL_SetEnergyMeterStatus(&data);
	
	//ln882  - flash_vars_store             --- ln_kv_set -- key/value/size ; set/get/has_key
	//w800   - write_flash_boot_content		--- tls_fls_write/tls_fls_read -- by address in firmware area
	//bk7231 - flash_vars_write				--- by address 
	//bl602  - BL602_SaveFlashVars			--- ef_set_env_blob	 --- easyflash lib/
	
#if PLATFORM_BL602
	//BL602_InitEasyFlashIfNeeded(); //Hope it already called.

	int dataLen = sizeof(data);
	EfErrCode res = ef_set_env_blob(EASYFLASH_EMERA, &data, dataLen);
	if (res == EF_ENV_INIT_FAILED) {
		addLogAdv(LOG_ERROR, LOG_FEATURE_ENERGYMETER, "ENERGYMERA_SaveEmetering: EF_ENV_INIT_FAILED for %d bytes", dataLen);
		return;
	}
	if (res == EF_ENV_ARG_ERR) {
		addLogAdv(LOG_ERROR, LOG_FEATURE_ENERGYMETER, "ENERGYMERA_SaveEmetering: EF_ENV_ARG_ERR for %d bytes", dataLen);
		return;
	}
	//ADDLOG_DEBUG(LOG_FEATURE_ENERGYMETER, "ENERGYMERA_SaveEmetering: saved %d bytes", dataLen);
	addLogAdv(LOG_INFO, LOG_FEATURE_ENERGYMETER, "ENERGYMERA_SaveEmetering: saved, %d bytes\n", dataLen);
#else
#error "Not implemented"
#endif
}