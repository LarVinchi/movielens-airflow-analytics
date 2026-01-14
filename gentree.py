import os

# Directories to strictly ignore
IGNORE_DIRS = {
    '.git', '.venv', '.idea', '.vscode', '__pycache__', 
    'logs', 'data', 'movielens_airflow_analytics.egg-info',
    'dag_processor_manager', 'scheduler', 'plugins'
}

# Files to strictly ignore
IGNORE_FILES = {
    '.DS_Store', '.env', 'gentree.py', 'airflow.db', 'airflow.cfg', 
    'webserver_config.py'
}

def generate_tree(startpath):
    print(f"{os.path.basename(os.path.abspath(startpath))}/")
    
    for root, dirs, files in os.walk(startpath):
        # Filter directories in-place so os.walk doesn't traverse them
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        
        # Sort to make output consistent
        dirs.sort()
        files.sort()
        
        level = root.replace(startpath, '').count(os.sep)
        indent = '│   ' * (level)
        subindent = '│   ' * (level) + '├── '
        
        # Don't print the root folder again
        if root != startpath:
            print(f"{indent}{os.path.basename(root)}/")
            
        for i, f in enumerate(files):
            if f not in IGNORE_FILES:
                # Use a different connector for the last item if you want to be fancy,
                # but standard '├──' is fine for Markdown blocks.
                print(f"{subindent}{f}")

if __name__ == "__main__":
    generate_tree(".")