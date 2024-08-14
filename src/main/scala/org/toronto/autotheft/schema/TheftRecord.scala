package org.toronto.autotheft.schema

case class TheftRecord(
                        X: Double,
                        Y: Double,
                        OBJECTID: Int,
                        EVENT_UNIQUE_ID: String,
                        REPORT_DATE: String,
                        OCC_DATE: String,
                        REPORT_YEAR: Int,
                        REPORT_MONTH: String,
                        REPORT_DAY: Int,
                        REPORT_DOY: Int,
                        REPORT_DOW: String,
                        REPORT_HOUR: Int,
                        OCC_YEAR: Int,
                        OCC_MONTH: String,
                        OCC_DAY: Int,
                        OCC_DOY: Int,
                        OCC_DOW: String,
                        OCC_HOUR: Int,
                        DIVISION: String,
                        LOCATION_TYPE: String
                      )