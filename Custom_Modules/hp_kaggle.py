import os as _os

def kaggle_api(username, api_key):
    """Creates a Kaggle API object which can download and search for datasets.

    Args:
        username (str): Kaggle username
        api_key (str): Kaggle API key

    Returns:
        Kaggle API object.
    """
    _os.environ['KAGGLE_USERNAME'] = username
    _os.environ['KAGGLE_KEY'] = api_key

    from kaggle.api.kaggle_api_extended import KaggleApi

    k_api = KaggleApi()
    k_api.authenticate()
    return k_api



def kaggle_dataset_download(kaggle_api, dataset, file_list, download_path, force=True, quiet=True):
    if not isinstance(file_list.__iter__(), object):
        print(f"Argument Error: file_list must be iterable object!")
    else:
        if not _os.path.exists(download_path):
            _os.makedirs(download_path)
        for file_ in file_list:
            kaggle_api.dataset_download_file(dataset=dataset, file_name=file_, path=download_path, force=force, quiet=quiet)
            print(f"{file_} successfully downloaded to {download_path}")



def kaggle_comp_download(kaggle_api, comp_name, file_list, download_path, force=True, quiet=True):
    if not isinstance(file_list.__iter__(), object):
        print(f"Argument Error: file_list must be iterable object!")
    else:
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        for file_ in file_list:
            kaggle_api.competition_download_file(competition=comp_name, file_name=file_, path=download_path, force=force, quiet=quiet)
            print(f"{file_} successfully downloaded to {download_path}")