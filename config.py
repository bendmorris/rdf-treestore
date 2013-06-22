import ConfigParser
import os
import sys


def get_treestore_kwargs():
    config_dir = os.path.expanduser('~/.treestore')
    if not os.path.exists(config_dir): os.makedirs(config_dir)
    config_file_path = os.path.join(config_dir, 'treestore.config')
    
    defaults = [
                ('dsn', 'Virtuoso'),
                ('user', 'dba'),
                ('password', 'dba'),
                ('load_dir', '/tmp/treestore'),
                ]
    
    config = ConfigParser.SafeConfigParser()
    if os.path.exists(config_file_path): 
        config.read(config_file_path)
    else:
        if not config.has_section('treestore'): config.add_section('treestore')
        for key, value in defaults:
            config.set('treestore', key, value)
            with open(config_file_path, 'w') as output_file:
                config.write(output_file)
    
    # store config options in a dictionary
    kwargs = {}
    for k, v in config.items('treestore'):
        kwargs[k] = v

    load_dir = kwargs['load_dir'] if 'load_dir' in kwargs else dict(defaults)['load_dir']
    if not os.path.exists(load_dir): os.makedirs(load_dir)
    
    return kwargs
