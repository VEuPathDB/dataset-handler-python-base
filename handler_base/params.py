from handler_base.dataset_handler import ValidationException


class Params:
    def __init__(self, args):
        if len(args) < 6:
            raise ValidationException("The tool was passed an insufficient numbers of arguments.")

        self.ds_name = args[0]
        self.ds_summary = args[1]
        self.ds_description = args[2]
        self.user_id = args[3]
        self.output_file = args[4]
        self.origin = args[5]
