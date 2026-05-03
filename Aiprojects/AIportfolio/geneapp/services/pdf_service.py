from reportlab.platypus import SimpleDocTemplate, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

def generate_pdf(text, filename="report.pdf"):

    doc = SimpleDocTemplate(filename)
    styles = getSampleStyleSheet()

    content = []

    for line in text.split("\n"):
        content.append(Paragraph(line, styles["Normal"]))

    doc.build(content)

    return filename