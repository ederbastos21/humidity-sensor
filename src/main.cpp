#include <Arduino.h>
#include <Wire.h>
#include <Adafruit_ADS1X15.h>

// Configuracao do hardware
#define SDA_PIN 21
#define SCL_PIN 22
#define ADS1_ADDRESS 0x48  // ADDR do primeiro ADS1115 no GND
#define ADS2_ADDRESS 0x49  // ADDR do segundo ADS1115 no VDD

// Intervalo entre coletas completas dos seis sensores
const unsigned long INTERVALO_LEITURA_MS = 2000;

Adafruit_ADS1115 ads1;
Adafruit_ADS1115 ads2;

unsigned long ultimaLeitura = 0;

void setup() {
    Serial.begin(115200);
    Wire.begin(SDA_PIN, SCL_PIN);

    if (!ads1.begin(ADS1_ADDRESS, &Wire)) {
        // Mensagens de erro nao seguem o protocolo CSV e serao ignoradas pelo Python.
        Serial.println("ERRO: ADS1 0x48 nao encontrado");
        while (true) delay(100);
    }

    if (!ads2.begin(ADS2_ADDRESS, &Wire)) {
        Serial.println("ERRO: ADS2 0x49 nao encontrado");
        while (true) delay(100);
    }

    ads1.setGain(GAIN_ONE);
    ads2.setGain(GAIN_ONE);
    ads1.setDataRate(RATE_ADS1115_128SPS);
    ads2.setDataRate(RATE_ADS1115_128SPS);

    // Faz a primeira coleta imediatamente.
    ultimaLeitura = millis() - INTERVALO_LEITURA_MS;
}

void loop() {
    const unsigned long agora = millis();

    if (agora - ultimaLeitura >= INTERVALO_LEITURA_MS) {
        ultimaLeitura = agora;

        // Uma linha CSV por coleta: capacitivo 1..3, resistivo 1..3.
        const int16_t cap1 = ads1.readADC_SingleEnded(1);
        const int16_t cap2 = ads1.readADC_SingleEnded(2);
        const int16_t cap3 = ads1.readADC_SingleEnded(3);

        const int16_t res1 = ads2.readADC_SingleEnded(1);
        const int16_t res2 = ads2.readADC_SingleEnded(2);
        const int16_t res3 = ads2.readADC_SingleEnded(3);

        Serial.printf(
            "%lu,%d,%d,%d,%d,%d,%d\n",
            millis(),
            cap1, cap2, cap3,
            res1, res2, res3
        );
    }
}
