import tempfile


def frame2file(frame):
    """
    Convert a DataFrame into a temporary file

    :param frame: a DataFrame
    :return: a file object
    """
    tmp_file = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, prefix="lwm_")
    frame.to_csv(tmp_file.name)
    tmp_file.close()
    return tmp_file
