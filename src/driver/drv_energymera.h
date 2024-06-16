#pragma once

#include "../httpserver/new_http.h"

void ENERGYMERA_Init(void);
void ENERGYMERA_RunEverySecond(void);
void ENERGYMERA_AppendInformationToHTTPIndexPage(http_request_t *request);
void ENERGYMERA_SaveEmeteringStatistics();
bool EMERA_PublishHASSDevices(const char* topic);