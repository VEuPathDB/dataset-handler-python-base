from dataset_handler import ValidationException


class Params:
    """
    Dataset Handler Parameters

    Args:
        args (list of str): List of string arguments

    Attributes:
        ds_name (str): Dataset Name
        ds_summary (str): Dataset Summary
        ds_description (str): Dataset Description
        user_id (str): WDK User ID
        output_file (str): Output file name
        origin (str): Dataset origin (either `galaxy` or `direct_upload`)

    Raises:
        ValidationException: If there are fewer than 6 arguments in the ``args``
            list.
    """

    def __init__(self, args):
        if len(args) < 6:
            raise ValidationException("The tool was passed an insufficient numbers of arguments.")

        self.ds_name = args[0]
        self.ds_summary = args[1]
        self.ds_description = args[2]
        self.user_id = args[3]
        self.output_file = args[4]
        self.origin = args[5]
