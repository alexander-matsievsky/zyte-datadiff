import unittest
from typing import Any

import zyte_datadiff
from zyte_datadiff.dataset import ScrapyCloudItem, ScrapyCloudJob


class CarSpiderDataDiff(zyte_datadiff.DataDiff):
    """
    .. code-block:: bash

        curl -s 'https://storage.scrapinghub.com/items/533722/1/390/0?apikey=...&format=json&meta=_key' | jq -S '.[0]'


    .. code-block:: json

        {
          "_key": "533722/1/390/0",
          "_type": "dict",
          "bodyType": "Limuzyna",
          "brand": "Kia",
          "dateItemScraped": "2022-03-28T00:01:46+00:00",
          "dateVehicleFirstRegistered": "2018-02-01",
          "emissionsCO2UnitCode": "g/km",
          "emissionsCO2Value": 109,
          "fuelType": "Benzyna",
          "identifier": "ae758740-f624-465c-a5bb-47c3903b0092",
          "image": "https://www.example.com/ae758740-f624-465c-a5bb-47c3903b0092.jpg",
          "inLanguage": "pl",
          "mileageFromOdometerUnitCode": "KMT",
          "mileageFromOdometerValue": 63082,
          "model": "Rio",
          "name": "Kia Rio",
          "numberOfDoors": 5,
          "offersEligibleRegion": "PL",
          "offersPrice": 47100,
          "offersPriceCurrency": "PLN",
          "seatingCapacity": 5,
          "source": "example.com",
          "type": "Car",
          "url": "https://www.example.com/ae758740-f624-465c-a5bb-47c3903b0092.html",
          "vehicleEngineDisplacementUnitCode": "CMQ",
          "vehicleEngineDisplacementValue": 1248,
          "vehicleEnginePowerUnitCode": "KWT",
          "vehicleEnginePowerValue": 62,
          "vehicleEngineType": "1.2",
          "vehicleInteriorColor": "Srebrny",
          "vehicleTransmission": "Manualna skrzynia biegów"
        }
    """

    def project_key(self, item: dict) -> Any:
        if not item.get("identifier"):
            return
        return item.get("type"), item.get("source"), item.get("identifier")

    def project_value(self, item: dict) -> dict:
        item.pop("_key", None)
        item.pop("dateItemScraped", None)
        item.pop("image", None)
        return item


class DataDiffTestCase(unittest.TestCase):
    @staticmethod
    def test_dataset_vs_dataset():
        data_diff = CarSpiderDataDiff(
            left=ScrapyCloudJob("https://app.zyte.com/p/533722/1/388")
        )
        for result in data_diff.compare(
                right=ScrapyCloudJob("https://app.zyte.com/p/533722/1/390")
        ):
            match result:
                case zyte_datadiff.DataDiff.LeftOnly(left=item):
                    print(f"item was deleted: {item}")
                case zyte_datadiff.DataDiff.LeftRightNotEqual(
                    diff=diff, left=prev, right=next_
                ):
                    print(f"item was modified: from {prev} to {next_} with {diff}")
                case zyte_datadiff.DataDiff.RightOnly(right=item):
                    print(f"item was added: {item}")

    @staticmethod
    def test_dataset_vs_item():
        data_diff = CarSpiderDataDiff(
            left=ScrapyCloudJob("https://app.zyte.com/p/533722/1/388")
        )
        match next(
            data_diff.compare(
                right=ScrapyCloudItem("https://app.zyte.com/p/533722/1/390/item/0")
            )
        ):
            case (
            zyte_datadiff.DataDiff.LeftRightNotEqual(right=item)
            | zyte_datadiff.DataDiff.RightOnly(right=item)
            ):
                print(
                    f"item is either brand new or has changed since last run. re-crawling {item}"
                )


if __name__ == "__main__":
    unittest.main()
