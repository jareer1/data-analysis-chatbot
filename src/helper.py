import re
import string
import streamlit as st
import io
import base64
import matplotlib.pyplot as plt

def display_text_with_images(text):
    """
    Display text with associated images.

    Args:
        text (str): The text to be displayed.

    Returns:
        None
    """

    # Modify the regex to remove potential '[voir image]' and parentheses around the URL
    image_urls = re.findall(
        r"https?://[^\s]+image[^\s]*.jpg",
        text,
        flags=re.IGNORECASE,
    )

    # Replace the markdown image syntax with just the URL for splitting
    text_for_splitting = re.sub(
        r"-? +?!?\[lien vers l'image\]\s*\(?(https?://[^\s]+image[^\s]*.jpg)\)?",
        r"\1 \n ",
        text,
        flags=re.IGNORECASE,
    )

    # Split text at image URLs
    parts = re.split(r"https?://[^\s]+image[^\s]*.jpg", text_for_splitting)

    for i, part in enumerate(parts):
        # If there is punctuation character, parts[i] must have at least one alpha character.
        if any(char in string.punctuation for char in part) and not any(
            char.isalpha() for char in part
        ):
            continue
        # Display the text part
        st.markdown(part.replace("\n", "\n\n"))

        # Display the image if it exists
        if i < len(image_urls):
            st.image(image_urls[i])





def display_python_code_plots(text):
    """
    Extract Python code from the given text and execute it to generate a plot.

    Args:
        text (str): Input text containing Python code for plotting.

    Returns:
        str: Path to the saved plot file (e.g., 'plot.png') or None if no plot is generated.
    """
    # Extract Python code from the markdown block
    pattern = r'```python\s(.*?)```'
    matches = re.findall(pattern, text, re.DOTALL)

    if not matches:
        return None

    python_code = matches[0]

    try:
        # Ensure non-interactive mode
        plt.ioff()

        # Prepare the code for execution by adding necessary imports
        code = "import pandas as pd\nimport matplotlib.pyplot as plt\n" + python_code.replace("fig.show()", "")
        code += "\nfig = plt.figure()\n"  # Ensure a figure object is created
        code += "\nfig.tight_layout()"  # Prevent layout issues if any
        code += "\n"

        # Execute the code
        exec_globals = {"io": io, "plt": plt}
        exec(code, exec_globals)

        # Save the plot locally to a file
        plot_filename = "generated_plot.png"  # You can specify any path you want
        exec_globals["fig"].savefig(plot_filename, format='png')

        # Close the plot to release memory
        plt.close(exec_globals["fig"])

        # Return the path to the saved plot
        return plot_filename
    except Exception as e:
        print(f"Error generating plot: {e}")
        return None
