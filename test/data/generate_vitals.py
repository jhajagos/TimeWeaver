import csv
import datetime
import random
import time


def main():

    number_of_measures = {
        "1": 20,
        "2": 10,
        "3": 5
    }

    average_pulse = {
        "1": 80,
        "2": 90,
        "3": 100
    }

    header = ["encounter_id", "vital_measurement_date_time", "vital_measurement_code","vital_measurement_name",
              "vital_measurment_value"]

    with open("vitals.csv", newline="", mode="w") as fw:

        csv_writer = csv.writer(fw)
        csv_writer.writerow(header)

        with open("encounters.csv", mode="r", newline="") as f:

            dict_reader = csv.DictReader(f)

            for row_dict in dict_reader:

                encounter_id = row_dict["encounter_id"]

                admit_date_time = row_dict["admit_date_time"]
                discharge_date_time = row_dict["discharge_date_time"]

                a_dts = datetime.datetime.strptime(admit_date_time, "%Y-%m-%d %H:%M")
                d_dts = datetime.datetime.strptime(discharge_date_time, "%Y-%m-%d %H:%M")

                span_dts = d_dts - a_dts

                for n in range(number_of_measures[encounter_id]):
                    measurement_date_time_gap = random.randint(0, int(span_dts.total_seconds()))
                    measurement_date_time = a_dts.timestamp() + measurement_date_time_gap

                    measurement_date_time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(measurement_date_time))

                    csv_writer.writerow([encounter_id, measurement_date_time_str, "56565-3", "pulse", str(int(random.normalvariate(average_pulse[encounter_id],5)))])




if __name__ == "__main__":

    main()