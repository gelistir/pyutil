from unittest import mock

import pandas as pd
from pyutil.config.folder import folder_stamped


def test_folder():
    with mock.patch('pyutil.config.folder.pathlib') as mock_path:
        folder_stamped("./", "tmp")
        mock_path.Path.assert_called_with("./tmp/{t}".format(t=pd.Timestamp("today").strftime("%Y%m%d")))

