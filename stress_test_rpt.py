from jinja2 import Environment, PackageLoader, select_autoescape


def render_report(template_name, report_path, opts, **kwargs):
    """
    
    :param kwargs: should  
    :return: 
    """
    env = Environment(
        loader=PackageLoader('reports', 'templates'),
        autoescape=select_autoescape(['html', 'xml'])
    )
    template = env.get_template(template_name)
    rendered_report = template.render(opts)

    try:
        with open(report_path, "w") as writefile:
            writefile.write(rendered_report)
    except(e):
        print("Error writing report")
        print(e)
