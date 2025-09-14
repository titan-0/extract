#!/usr/bin/env python3

import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
import os

def create_comprehensive_fish_pdf():
    """Create a comprehensive fish data PDF with all required database columns"""
    
    filename = "comprehensive_fish_data_2024.pdf"
    filepath = os.path.join("test_files2", filename)
    
    doc = SimpleDocTemplate(filepath, pagesize=letter,
                          rightMargin=50, leftMargin=50,
                          topMargin=72, bottomMargin=18)
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], 
                                fontSize=14, spaceAfter=20, alignment=TA_CENTER)
    
    story = []
    
    # Title
    title = Paragraph("Comprehensive Fish Survey Database 2024", title_style)
    story.append(title)
    
    # Complete fish data with all database columns
    fish_data = [
        ['scientificName', 'species', 'class', 'family', 'kingdom', 'phylum', 'habitat', 'diet_type', 'reproductive_type', 'decimalLatitude', 'decimalLongitude', 'locality', 'fishing_region', 'lifespan_years', 'migration_patterns'],
        ['Gadus morhua', 'Atlantic Cod', 'Actinopterygii', 'Gadidae', 'Animalia', 'Chordata', 'marine', 'carnivore', 'oviparous', '57.2341', '2.4567', 'North Sea', 'ICES_IVa', '12.5', 'Seasonal'],
        ['Clupea harengus', 'Atlantic Herring', 'Actinopterygii', 'Clupeidae', 'Animalia', 'Chordata', 'marine', 'planktivore', 'oviparous', '56.1892', '2.7834', 'North Sea', 'ICES_IVb', '6.2', 'Long-distance'],
        ['Pleuronectes platessa', 'European Plaice', 'Actinopterygii', 'Pleuronectidae', 'Animalia', 'Chordata', 'marine', 'carnivore', 'oviparous', '55.9876', '3.1245', 'North Sea', 'ICES_IVa', '10.8', 'Spawning migration'],
        ['Pollachius virens', 'Pollock', 'Actinopterygii', 'Gadidae', 'Animalia', 'Chordata', 'marine', 'carnivore', 'oviparous', '55.7654', '2.8901', 'North Sea', 'ICES_VIa', '8.3', 'Short-distance'],
        ['Scomber scombrus', 'Atlantic Mackerel', 'Actinopterygii', 'Scombridae', 'Animalia', 'Chordata', 'marine', 'carnivore', 'oviparous', '55.5432', '3.4567', 'Celtic Sea', 'ICES_VIIa', '7.1', 'Seasonal'],
        ['Solea solea', 'Common Sole', 'Actinopterygii', 'Soleidae', 'Animalia', 'Chordata', 'marine', 'carnivore', 'oviparous', '56.0123', '2.5678', 'North Sea', 'ICES_IVc', '9.4', 'Seasonal'],
        ['Salmo salar', 'Atlantic Salmon', 'Actinopterygii', 'Salmonidae', 'Animalia', 'Chordata', 'marine', 'carnivore', 'oviparous', '58.2341', '1.4567', 'Norwegian Sea', 'ICES_IIa', '8.7', 'Anadromous'],
        ['Thunnus thynnus', 'Bluefin Tuna', 'Actinopterygii', 'Scombridae', 'Animalia', 'Chordata', 'marine', 'carnivore', 'oviparous', '49.7654', '-8.8901', 'Celtic Sea', 'ICES_VIIj', '15.2', 'Long-distance']
    ]
    
    # Create table with smaller font to fit all columns
    fish_table = Table(fish_data)
    fish_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 6),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightblue),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 5),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
    ]))
    
    story.append(fish_table)
    story.append(Spacer(1, 20))
    
    # Add comprehensive oceanography data
    story.append(Paragraph("Oceanographic Measurements", styles['Heading2']))
    
    ocean_data = [
        ['data_set', 'version', 'decimalLatitude', 'decimalLongitude', 'maximumDepthInMeters', 'waterTemperature', 'salinity', 'dissolvedOxygen', 'water_pH', 'chlorophyll_mg_m3', 'nutrients', 'alkalinity'],
        ['COMPREHENSIVE_2024', 'v1.0_001', '57.2341', '2.4567', '45.0', '12.3', '34.9', '8.7', '8.1', '2.4', '{"nitrate": 12.5, "phosphate": 0.89}', '2250'],
        ['COMPREHENSIVE_2024', 'v1.0_002', '56.1892', '2.7834', '62.0', '11.8', '34.7', '9.2', '8.0', '1.8', '{"nitrate": 13.2, "phosphate": 0.95}', '2280'],
        ['COMPREHENSIVE_2024', 'v1.0_003', '55.9876', '3.1245', '38.0', '13.1', '35.1', '8.4', '8.2', '3.1', '{"nitrate": 10.8, "phosphate": 0.76}', '2320'],
        ['COMPREHENSIVE_2024', 'v1.0_004', '55.7654', '2.8901', '78.0', '10.9', '34.6', '9.8', '7.9', '1.2', '{"nitrate": 14.7, "phosphate": 1.02}', '2190'],
        ['COMPREHENSIVE_2024', 'v1.0_005', '55.5432', '3.4567', '55.0', '12.7', '34.8', '8.9', '8.1', '2.6', '{"nitrate": 11.9, "phosphate": 0.83}', '2270'],
        ['COMPREHENSIVE_2024', 'v1.0_006', '58.2341', '1.4567', '125.0', '8.4', '35.0', '10.1', '8.0', '1.5', '{"nitrate": 15.3, "phosphate": 1.08}', '2340'],
        ['COMPREHENSIVE_2024', 'v1.0_007', '49.7654', '-8.8901', '180.0', '14.2', '35.3', '7.8', '8.3', '3.8', '{"nitrate": 9.6, "phosphate": 0.67}', '2180']
    ]
    
    ocean_table = Table(ocean_data)
    ocean_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkgreen),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 6),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightgreen),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 5),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
    ]))
    
    story.append(ocean_table)
    
    doc.build(story)
    print(f"✅ Created comprehensive data PDF: {filepath}")
    return filepath

if __name__ == "__main__":
    os.makedirs("test_files2", exist_ok=True)
    create_comprehensive_fish_pdf()