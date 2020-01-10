import os

from pyutil.config.folder import folder_stamped


def test_folder():
    folder = folder_stamped("./", "tmp")
    assert os.path.exists(folder)
