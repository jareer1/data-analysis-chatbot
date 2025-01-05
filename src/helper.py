import string
import streamlit as st
import re
import base64
import io
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
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
    Extracts Python code for plotting from the input text, executes it,
    and returns the plot as a Base64-encoded image string.
    """
    # Match Python code block containing plot logic
    pattern = r'```python\s(.*?)```'
    matches = re.findall(pattern, text, re.DOTALL)
    if not matches:
        return None  # Return None if no code block is found

    code = matches[0]
    try:
        # Add required imports and logic
        code = """import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import plotly.io as pio
""" + code

        # Replace `fig.show()` with logic to export image
        code = code.replace("fig.show()", "")
        code += """# Export the plot to an image in memory using plotly.io
image_bytes = pio.to_image(fig, format="png")
# Convert the image bytes to Base64 for frontend use
plot_base64 = base64.b64encode(image_bytes).decode("utf-8")
        """

        # Prepare globals and locals for secure code execution
        exec_globals = {"io": io, "base64": base64, "pio": pio, "go": go, "px": px, "pd": pd}
        exec_locals = {}
        exec(code, exec_globals, exec_locals)

        # Retrieve Base64-encoded image from executed code
        return exec_locals.get("plot_base64", None)

    except Exception as e:
        print(f"Error executing code: {e}")
        return None

