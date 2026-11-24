"""Packing slip PDF for one FBA inbound shipment (layout approved by Cassie on 2026-11-24; dates left blank)."""
import io
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer

def build(shipment_id, name, fc, items, packed_by="Yehuda Kopp", checked_by="Yehuda Kopp", date_text=""):
    small = ParagraphStyle("s", fontName="Helvetica", fontSize=8.5, leading=10.5)
    cell = ParagraphStyle("c", fontName="Helvetica", fontSize=8.5, leading=10.5)
    bold = ParagraphStyle("b", fontName="Helvetica-Bold", fontSize=9, leading=11)
    title = ParagraphStyle("t", fontName="Helvetica-Bold", fontSize=20, alignment=2, leading=24)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter, leftMargin=0.6*inch, rightMargin=0.6*inch,
                            topMargin=0.55*inch, bottomMargin=0.9*inch, title=f"Packing Slip {shipment_id}")
    total = sum(q for _, _, q, _ in items)
    head = Table([[Paragraph("<b>123 Glasses LLC</b><br/>1874 Lanes Mill Rd<br/>Lakewood NJ 08701", small),
                   Paragraph("Packing Slip", title)]], colWidths=[3.6*inch, 3.7*inch])
    left = Paragraph(f"<b>Ship To:</b><br/>Amazon Fulfillment Center {fc}<br/>FBA inbound shipment", small)
    right = Table([["Date:", date_text], ["Customer ID:", "A45ZMV522SPKA"], ["Purchase Order #:", name],
                   ["FBA Shipment #:", shipment_id], ["Packager:", packed_by], ["Packing Date:", date_text]],
                  colWidths=[1.25*inch, 2.2*inch])
    right.setStyle(TableStyle([("FONT", (0,0), (0,-1), "Helvetica-Bold", 8.5), ("FONT", (1,0), (1,-1), "Helvetica", 8.5),
                               ("BOTTOMPADDING", (0,0), (-1,-1), 2), ("TOPPADDING", (0,0), (-1,-1), 2),
                               ("LINEBELOW", (1,0), (1,-1), 0.4, colors.grey)]))
    info = Table([[left, right]], colWidths=[3.7*inch, 3.6*inch])
    info.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP")]))
    rows = [["Item #", "Description", "Quantity", "FNSKU"]]
    for sku, desc, qty, fnsku in items:
        rows.append([Paragraph(sku, cell), Paragraph(desc, cell), str(qty), fnsku])
    t = Table(rows, colWidths=[2.35*inch, 3.15*inch, 0.75*inch, 1.05*inch], repeatRows=1)
    t.setStyle(TableStyle([("FONT", (0,0), (-1,0), "Helvetica-Bold", 9), ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E6E6E6")),
                           ("FONT", (2,1), (-1,-1), "Helvetica", 8.5), ("ALIGN", (2,0), (2,-1), "CENTER"),
                           ("GRID", (0,0), (-1,-1), 0.4, colors.grey), ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                           ("TOPPADDING", (0,0), (-1,-1), 3), ("BOTTOMPADDING", (0,0), (-1,-1), 3)]))
    foot = Table([["Special Notes:", "", "Total Quantity of Goods:", str(total)],
                  ["", "", "Packed By:", packed_by], ["", "", "Checked By:", checked_by], ["", "", "Signature:", ""]],
                 colWidths=[1.1*inch, 2.6*inch, 1.9*inch, 1.7*inch])
    foot.setStyle(TableStyle([("FONT", (0,0), (-1,-1), "Helvetica", 9), ("FONT", (2,0), (2,-1), "Helvetica-Bold", 9),
                              ("FONT", (0,0), (0,0), "Helvetica-Bold", 9), ("LINEBELOW", (3,0), (3,-1), 0.4, colors.grey),
                              ("TOPPADDING", (0,0), (-1,-1), 5)]))
    def footer(canvas, d):
        canvas.setFont("Helvetica", 8)
        canvas.drawCentredString(letter[0]/2, 0.55*inch, "123 Glasses LLC  1874 Lanes Mill Rd  Lakewood NJ 08701")
        canvas.drawCentredString(letter[0]/2, 0.4*inch, "Tel: 732-363-7505   Email: info@gefenoptical.com   Website: gefenoptical.com")
        canvas.drawRightString(letter[0]-0.6*inch, 0.4*inch, f"Page {d.page}")
    doc.build([head, Spacer(1, 10), info, Spacer(1, 14), t, Spacer(1, 12), foot], onFirstPage=footer, onLaterPages=footer)
    return buf.getvalue()
