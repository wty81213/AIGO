from asyncore import write
import xml.etree.ElementTree as ET
import csv
import glob
import os

files_path = glob.glob("data/xml/*.xml" )

statisticalMethods = ['日期'] # csv 欄位

with open('data/open_weather_data/output_2016-2018.csv', 'w', newline='') as csvfile:

    writer = csv.writer(csvfile)

    for i, file_path in enumerate(files_path):

        mytree = ET.parse(file_path)

        myroot = mytree.getroot()

        resource =  myroot.find('{urn:cwb.gov.tw:cwbdata-0.1}resources').find('{urn:cwb.gov.tw:cwbdata-0.1}resource')

        data = resource.findall('.//{urn:cwb.gov.tw:cwbdata-0.1}location')

        # csv欄位只做一次
        if i == 0:

            metadata = resource.find('{urn:cwb.gov.tw:cwbdata-0.1}metadata')

            statics = metadata.find('{urn:cwb.gov.tw:cwbdata-0.1}statistics')

            weatherElements = statics.find('{urn:cwb.gov.tw:cwbdata-0.1}weatherElements')

            for weatherElement in weatherElements: 

                description = weatherElement.findall('{urn:cwb.gov.tw:cwbdata-0.1}description')[0].text

                for statisticalMethod in weatherElement.find('{urn:cwb.gov.tw:cwbdata-0.1}statisticalMethods'):
                    # print(statisticalMethod.find('{urn:cwb.gov.tw:cwbdata-0.1}description').text)
                    statisticalMethod = statisticalMethod.find('{urn:cwb.gov.tw:cwbdata-0.1}description').text

                    statisticalMethods.append(statisticalMethod)

            writer.writerow(statisticalMethods)

        datas = []
        
        for d in data:
            stationName = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}stationName').text
            
            if stationName == "臺北":
                #日期
                dataYearMonth = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}dataYearMonth').text
                dataYearMonthRename = dataYearMonth.split('-')[1] + '-' + dataYearMonth.split('-')[0]
                datas.append(dataYearMonthRename)

                #溫度
                meanTemperature = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}temperature//{urn:cwb.gov.tw:cwbdata-0.1}mean').text
                datas.append(meanTemperature)
                maximumTemperature = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}temperature//{urn:cwb.gov.tw:cwbdata-0.1}maximum').text
                datas.append(maximumTemperature)
                maximumTemperatureDate = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}temperature//{urn:cwb.gov.tw:cwbdata-0.1}maximumDate').text
                datas.append(maximumTemperatureDate)
                minimumTemperature = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}minimum').text
                datas.append(minimumTemperature)
                minimumTemperatureDate = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}temperature//{urn:cwb.gov.tw:cwbdata-0.1}minimumDate').text
                datas.append(minimumTemperatureDate)

                #降雨
                accumulationPrecipitation = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}precipitation//{urn:cwb.gov.tw:cwbdata-0.1}accumulation').text
                datas.append(accumulationPrecipitation)
                GE01DaysPrecipitation = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}precipitation//{urn:cwb.gov.tw:cwbdata-0.1}GE01Days').text
                datas.append(GE01DaysPrecipitation)
                # print(accumulationPrecipitation)

                #風速
                maximumWindSpeed = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}windSpeed//{urn:cwb.gov.tw:cwbdata-0.1}maximum').text
                datas.append(maximumWindSpeed)
                maximumWindSpeedDate = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}windSpeed//{urn:cwb.gov.tw:cwbdata-0.1}maximumDate').text
                datas.append(maximumWindSpeedDate)

                #風向
                maximumWindDirection = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}windDirection//{urn:cwb.gov.tw:cwbdata-0.1}maximum').text
                datas.append(maximumWindDirection)

                #瞬間風速
                maximumGustSpeed = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}gustSpeed//{urn:cwb.gov.tw:cwbdata-0.1}maximum').text
                datas.append(maximumGustSpeed)
                maximumGustSpeedDate = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}gustSpeed//{urn:cwb.gov.tw:cwbdata-0.1}maximumDate').text
                datas.append(maximumGustSpeedDate)

                #瞬間風向
                maximumGustDirection = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}gustDirection//{urn:cwb.gov.tw:cwbdata-0.1}maximum').text
                datas.append(maximumGustDirection)

                #相對溼度
                meanRelativeHumidity = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}relativeHumidity//{urn:cwb.gov.tw:cwbdata-0.1}mean').text
                datas.append(meanRelativeHumidity)
                minimumRelativeHumidity = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}relativeHumidity//{urn:cwb.gov.tw:cwbdata-0.1}minimum').text
                datas.append(minimumRelativeHumidity)
                minimumRelativeHumidityDate = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}relativeHumidity//{urn:cwb.gov.tw:cwbdata-0.1}minimumDate').text
                datas.append(minimumRelativeHumidityDate)

                #氣壓
                pressure = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}stationPressure//{urn:cwb.gov.tw:cwbdata-0.1}mean').text
                datas.append(pressure)

                #日照時數
                sunshineDuration = d.find('.//{urn:cwb.gov.tw:cwbdata-0.1}sunshineDuration//{urn:cwb.gov.tw:cwbdata-0.1}total').text
                datas.append(sunshineDuration)

                writer.writerow(datas)
