# Ensure that running `python -m plsr` also uses the ephemeral venv.
from plsr.bootstrap import main

if __name__ == "__main__":
    main()
