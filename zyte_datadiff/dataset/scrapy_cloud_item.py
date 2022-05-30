from zyte_datadiff.dataset.dataset import Dataset


class ScrapyCloudItem(Dataset):
    url: str

    def __init__(self, url: str):
        self.url = url
