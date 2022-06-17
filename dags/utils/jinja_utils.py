from jinja2 import Environment, FileSystemLoader
import pathlib
import os
import mimetypes


def render_template(project: str, **kwargs) -> str:
    """
    :param project:
    :param kwargs: Jinja Template Vars
    :return: Rendered Template
    """
    env = Environment(loader=FileSystemLoader(searchpath=f'{os.path.dirname(__file__)}/templates'))
    template = env.get_template(kwargs.get('template_name'))
    return template.render(project=project, **kwargs)



def save_config_to_file(
        config: str,
        config_dir: pathlib.Path,
        file_name: str
) -> None:
    """
    :param config: Rendered Template
    :param config_dir:
    :param file_name:
    :return:
    """
    try:
        # mimetypes.init()
        config_dir.mkdir(parents=True, exist_ok=True)
        config_file = (config_dir / file_name)

        # save the files using the paths created
        with config_file.open(mode="w+") as f:
            f.write(config)
    except FileNotFoundError as e:  # noqa
        print("Is your Path Set Correctly")
        raise SystemExit(1)
