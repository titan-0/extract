#!/usr/bin/env python3

import pandas as pd
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
import os
import random
from datetime import datetime, timedelta
import numpy as np

def create_fish_survey_pdf():
    """Create a realistic fish survey PDF with tables and text"""
    
    filename = "fish_survey_north_sea_2024.pdf"
    filepath = os.path.join("test_files2", filename)
    
    # Create the PDF document
    doc = SimpleDocTemplate(filepath, pagesize=letter,
                          rightMargin=72, leftMargin=72,
                          topMargin=72, bottomMargin=18)
    
    # Get styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], 
                                fontSize=16, spaceAfter=30, alignment=TA_CENTER)
    heading_style = ParagraphStyle('CustomHeading', parent=styles['Heading2'], 
                                  fontSize=12, spaceAfter=12)
    normal_style = styles['Normal']
    
    # Build story
    story = []
    
    # Title
    title = Paragraph("Fish Species Survey - North Sea Region 2024", title_style)
    story.append(title)
    
    # Introduction text
    intro_text = """
    This report presents the findings of the 2024 fish species survey conducted in the North Sea region. 
    The survey was carried out between April and September 2024 as part of the ongoing marine biodiversity 
    monitoring program. A total of 45 sampling stations were established across the survey area, with 
    depth ranges from 15 to 200 meters.
    
    The primary objectives of this survey were to:
    • Assess the current abundance and distribution of commercial fish species
    • Monitor changes in species composition compared to previous years
    • Evaluate the health of fish populations in relation to environmental conditions
    • Collect baseline data for ecosystem management decisions
    """
    
    story.append(Paragraph(intro_text, normal_style))
    story.append(Spacer(1, 20))
    
    # Fish catch data table with database-compatible column names
    story.append(Paragraph("Table 1: Fish Species Data", heading_style))
    
    fish_data = [
        ['scientificName', 'species', 'family', 'habitat', 'diet_type', 'lifespan_years', 'decimalLatitude', 'decimalLongitude'],
        ['Gadus morhua', 'Atlantic Cod', 'Gadidae', 'marine', 'carnivore', '12.0', '56.2341', '2.4567'],
        ['Clupea harengus', 'Atlantic Herring', 'Clupeidae', 'marine', 'planktivore', '6.0', '56.1892', '2.7834'],
        ['Pleuronectes platessa', 'European Plaice', 'Pleuronectidae', 'marine', 'carnivore', '10.0', '55.9876', '3.1245'],
        ['Pollachius virens', 'Pollock', 'Gadidae', 'marine', 'carnivore', '8.0', '55.7654', '2.8901'],
        ['Scomber scombrus', 'Atlantic Mackerel', 'Scombridae', 'marine', 'carnivore', '7.0', '55.5432', '3.4567'],
        ['Melanogrammus aeglefinus', 'Haddock', 'Gadidae', 'marine', 'carnivore', '7.0', '55.3210', '2.9876'],
        ['Solea solea', 'Common Sole', 'Soleidae', 'marine', 'carnivore', '9.0', '56.0123', '2.5678'],
        ['Limanda limanda', 'Common Dab', 'Pleuronectidae', 'marine', 'carnivore', '4.0', '55.8765', '3.2109']
    ]
    
    fish_table = Table(fish_data, colWidths=[1.5*inch, 1.2*inch, 1.0*inch, 0.8*inch, 0.9*inch, 0.8*inch, 0.9*inch, 0.9*inch])
    fish_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8)
    ]))
    
    story.append(fish_table)
    story.append(Spacer(1, 20))
    
    # Environmental conditions section
    story.append(Paragraph("Environmental Conditions", heading_style))
    
    env_text = """
    Water quality measurements were taken at each sampling station. Temperature ranged from 8.2°C to 16.7°C, 
    with higher temperatures recorded in shallow coastal areas. Salinity levels were consistent across the 
    survey area, averaging 34.8 ppt. Dissolved oxygen concentrations were generally good, ranging from 
    7.2 to 11.4 mg/L.
    """
    
    story.append(Paragraph(env_text, normal_style))
    story.append(Spacer(1, 15))
    
    # Environmental data table with database-compatible column names
    story.append(Paragraph("Table 2: Oceanographic Measurements", heading_style))
    
    env_data = [
        ['data_set', 'version', 'decimalLatitude', 'decimalLongitude', 'maximumDepthInMeters', 'waterTemperature', 'salinity', 'dissolvedOxygen', 'water_pH'],
        ['NORTH_SEA_SURVEY', 'v2024.1_001', '56.2341', '2.4567', '45', '12.3', '34.9', '8.7', '8.1'],
        ['NORTH_SEA_SURVEY', 'v2024.1_002', '56.1892', '2.7834', '62', '11.8', '34.7', '9.2', '8.0'],
        ['NORTH_SEA_SURVEY', 'v2024.1_003', '55.9876', '3.1245', '38', '13.1', '35.1', '8.4', '8.2'],
        ['NORTH_SEA_SURVEY', 'v2024.1_004', '55.7654', '2.8901', '78', '10.9', '34.6', '9.8', '7.9'],
        ['NORTH_SEA_SURVEY', 'v2024.1_005', '55.5432', '3.4567', '55', '12.7', '34.8', '8.9', '8.1'],
        ['NORTH_SEA_SURVEY', 'v2024.1_006', '55.3210', '2.9876', '42', '13.8', '35.0', '8.1', '8.3']
    ]
    
    env_table = Table(env_data, colWidths=[1.2*inch, 1.0*inch, 1.0*inch, 1.0*inch, 1.2*inch, 1.0*inch, 0.8*inch, 1.0*inch, 0.8*inch])
    env_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightcyan),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8)
    ]))
    
    story.append(env_table)
    story.append(Spacer(1, 20))
    
    # Conclusions
    story.append(Paragraph("Conclusions and Recommendations", heading_style))
    
    conclusion_text = """
    The 2024 North Sea fish survey revealed several important findings:
    
    1. Species Diversity: A total of 23 fish species were recorded, showing good biodiversity.
    2. Population Health: Most commercial species showed stable population levels compared to 2023.
    3. Environmental Quality: Water quality parameters were within acceptable ranges for marine life.
    4. Climate Impact: Slight temperature increases observed in shallow areas may affect species distribution.
    
    Recommendations for future monitoring include increased sampling frequency in coastal zones and 
    continued assessment of climate change impacts on fish populations.
    """
    
    story.append(Paragraph(conclusion_text, normal_style))
    
    # Build PDF
    doc.build(story)
    print(f"✅ Created fish survey PDF: {filepath}")
    return filepath

def create_oceanography_data_pdf():
    """Create a realistic oceanography data PDF"""
    
    filename = "oceanographic_measurements_atlantic_2024.pdf"
    filepath = os.path.join("test_files2", filename)
    
    doc = SimpleDocTemplate(filepath, pagesize=A4,
                          rightMargin=72, leftMargin=72,
                          topMargin=72, bottomMargin=18)
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], 
                                fontSize=18, spaceAfter=30, alignment=TA_CENTER)
    heading_style = ParagraphStyle('CustomHeading', parent=styles['Heading2'], 
                                  fontSize=12, spaceAfter=12)
    
    story = []
    
    # Title
    title = Paragraph("Oceanographic Data Collection Report<br/>North Atlantic Survey 2024", title_style)
    story.append(title)
    
    # Introduction
    intro = """
    This document contains oceanographic measurements collected during the North Atlantic research cruise 
    aboard R/V Atlantic Explorer from June 15-30, 2024. Data collection focused on water column properties 
    including temperature, salinity, dissolved oxygen, pH, and nutrient concentrations across 18 stations.
    """
    
    story.append(Paragraph(intro, styles['Normal']))
    story.append(Spacer(1, 20))
    
    # CTD Data Table with database-compatible column names
    story.append(Paragraph("Oceanographic Data Collection", heading_style))
    
    ctd_data = [
        ['data_set', 'version', 'decimalLatitude', 'decimalLongitude', 'maximumDepthInMeters', 'waterTemperature', 'salinity', 'dissolvedOxygen', 'water_pH'],
        ['ATLANTIC_CTD', 'v2024.6_001', '48.7654', '-12.3456', '150', '9.8', '35.2', '8.9', '8.1'],
        ['ATLANTIC_CTD', 'v2024.6_002', '49.1234', '-11.9876', '225', '8.3', '35.1', '9.4', '8.0'],
        ['ATLANTIC_CTD', 'v2024.6_003', '49.5678', '-11.5432', '180', '9.1', '35.3', '8.7', '8.2'],
        ['ATLANTIC_CTD', 'v2024.6_004', '50.0123', '-11.1098', '310', '7.2', '35.0', '10.1', '7.9'],
        ['ATLANTIC_CTD', 'v2024.6_005', '50.4567', '-10.7654', '95', '11.4', '35.4', '7.8', '8.3'],
        ['ATLANTIC_CTD', 'v2024.6_006', '50.8901', '-10.3210', '275', '7.8', '35.1', '9.6', '8.1'],
        ['ATLANTIC_CTD', 'v2024.6_007', '51.2345', '-9.8765', '420', '6.1', '34.9', '10.8', '7.8'],
        ['ATLANTIC_CTD', 'v2024.6_008', '51.6789', '-9.4321', '195', '8.9', '35.2', '8.5', '8.2']
    ]
    
    ctd_table = Table(ctd_data, colWidths=[1.0*inch, 1.0*inch, 1.0*inch, 1.0*inch, 1.2*inch, 1.0*inch, 0.8*inch, 1.0*inch, 0.8*inch])
    ctd_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightsteelblue),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 7)
    ]))
    
    story.append(ctd_table)
    story.append(Spacer(1, 20))
    
    # Chemical analysis section
    story.append(Paragraph("Water Chemistry Analysis", heading_style))
    
    chem_text = """
    Water samples were collected at standard depths and analyzed for dissolved oxygen, pH, and major nutrients. 
    Dissolved oxygen levels ranged from 6.8 to 9.4 mg/L, indicating good water quality. pH values were slightly 
    alkaline, ranging from 7.9 to 8.3, which is typical for open ocean waters.
    """
    
    story.append(Paragraph(chem_text, styles['Normal']))
    story.append(Spacer(1, 15))
    
    # Chemistry data table with additional oceanographic parameters
    story.append(Paragraph("Additional Water Chemistry Data", heading_style))
    
    chem_data = [
        ['data_set', 'version', 'decimalLatitude', 'decimalLongitude', 'chlorophyll_mg_m3', 'nutrients', 'alkalinity'],
        ['ATLANTIC_CHEM', 'v2024.6_009', '48.7654', '-12.3456', '2.1', '{"nitrate": 12.4, "phosphate": 0.85}', '2250'],
        ['ATLANTIC_CHEM', 'v2024.6_010', '49.1234', '-11.9876', '1.8', '{"nitrate": 14.1, "phosphate": 0.92}', '2280'],
        ['ATLANTIC_CHEM', 'v2024.6_011', '49.5678', '-11.5432', '2.4', '{"nitrate": 11.8, "phosphate": 0.78}', '2230'],
        ['ATLANTIC_CHEM', 'v2024.6_012', '50.0123', '-11.1098', '1.2', '{"nitrate": 15.7, "phosphate": 1.05}', '2320'],
        ['ATLANTIC_CHEM', 'v2024.6_013', '50.4567', '-10.7654', '3.1', '{"nitrate": 9.3, "phosphate": 0.65}', '2180'],
        ['ATLANTIC_CHEM', 'v2024.6_014', '50.8901', '-10.3210', '1.6', '{"nitrate": 13.9, "phosphate": 0.89}', '2290'],
        ['ATLANTIC_CHEM', 'v2024.6_015', '51.2345', '-9.8765', '0.9', '{"nitrate": 16.2, "phosphate": 1.12}', '2350'],
        ['ATLANTIC_CHEM', 'v2024.6_016', '51.6789', '-9.4321', '2.3', '{"nitrate": 12.1, "phosphate": 0.81}', '2240']
    ]
    
    chem_table = Table(chem_data, colWidths=[1.0*inch, 1.0*inch, 1.0*inch, 1.0*inch, 1.2*inch, 1.8*inch, 1.0*inch])
    chem_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.green),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightgreen),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8)
    ]))
    
    story.append(chem_table)
    story.append(PageBreak())
    
    # Page 2 - Additional data
    story.append(Paragraph("Current and Wave Measurements", heading_style))
    
    current_text = """
    Surface current measurements were recorded using ADCP (Acoustic Doppler Current Profiler) at each station. 
    Current speeds ranged from 0.15 to 0.78 m/s, with predominantly southwestward flow patterns observed across 
    the survey area. Wave heights varied from 1.2 to 2.8 meters.
    """
    
    story.append(Paragraph(current_text, styles['Normal']))
    story.append(Spacer(1, 15))
    
    # Current data table
    current_data = [
        ['Station', 'Current Speed (m/s)', 'Current Direction (°)', 'Wave Height (m)', 'Wave Period (s)', 'Wind Speed (m/s)'],
        ['ATL-001', '0.34', '225', '1.8', '6.2', '8.4'],
        ['ATL-002', '0.42', '235', '2.1', '7.1', '9.8'],
        ['ATL-003', '0.28', '215', '1.5', '5.8', '7.2'],
        ['ATL-004', '0.56', '240', '2.5', '8.3', '12.1'],
        ['ATL-005', '0.19', '205', '1.2', '5.1', '6.5'],
        ['ATL-006', '0.48', '230', '2.2', '7.6', '10.3'],
        ['ATL-007', '0.67', '245', '2.8', '9.1', '14.2'],
        ['ATL-008', '0.31', '220', '1.7', '6.5', '8.9']
    ]
    
    current_table = Table(current_data, colWidths=[0.8*inch, 1.2*inch, 1.2*inch, 1.0*inch, 1.0*inch, 1.1*inch])
    current_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.navy),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightblue),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8)
    ]))
    
    story.append(current_table)
    
    doc.build(story)
    print(f"✅ Created oceanography PDF: {filepath}")
    return filepath

def create_marine_ecology_pdf():
    """Create a marine ecology research PDF with complex text and tables"""
    
    filename = "marine_ecology_biodiversity_study_2024.pdf"
    filepath = os.path.join("test_files2", filename)
    
    doc = SimpleDocTemplate(filepath, pagesize=letter,
                          rightMargin=72, leftMargin=72,
                          topMargin=72, bottomMargin=18)
    
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], 
                                fontSize=16, spaceAfter=30, alignment=TA_CENTER)
    
    story = []
    
    # Title
    title = Paragraph("Marine Biodiversity Assessment<br/>Celtic Sea Ecosystem Study", title_style)
    story.append(title)
    
    # Abstract
    abstract_text = """
    <b>ABSTRACT</b><br/><br/>
    This study presents a comprehensive assessment of marine biodiversity in the Celtic Sea ecosystem. 
    Over a 6-month period, we conducted systematic surveys of fish, invertebrate, and plankton communities 
    across 25 sampling stations. Our findings indicate high species richness with 67 fish species, 
    34 invertebrate taxa, and diverse phytoplankton communities recorded. Temperature and salinity 
    gradients were identified as primary drivers of species distribution patterns.
    """
    
    story.append(Paragraph(abstract_text, styles['Normal']))
    story.append(Spacer(1, 20))
    
    # Species composition table
    story.append(Paragraph("Table 1: Fish Species Composition by Family", styles['Heading2']))
    
    species_data = [
        ['Family', 'Species Count', 'Abundance (%)', 'Biomass (kg)', 'Trophic Level', 'Conservation Status'],
        ['Gadidae', '8', '23.4', '145.7', '3.8', 'LC'],
        ['Clupeidae', '6', '31.2', '89.3', '2.9', 'LC'],
        ['Pleuronectidae', '9', '18.7', '67.4', '3.2', 'LC'],
        ['Scombridae', '4', '12.1', '98.6', '4.2', 'NT'],
        ['Sparidae', '7', '8.9', '45.2', '3.1', 'LC'],
        ['Labridae', '12', '4.3', '23.8', '2.7', 'LC'],
        ['Gobiidae', '15', '1.2', '8.9', '2.4', 'LC'],
        ['Rajidae', '6', '0.2', '156.3', '3.9', 'VU']
    ]
    
    species_table = Table(species_data, colWidths=[1.2*inch, 0.9*inch, 0.9*inch, 0.9*inch, 0.9*inch, 1.0*inch])
    species_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkgreen),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightgreen),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8)
    ]))
    
    story.append(species_table)
    story.append(Spacer(1, 15))
    
    # Environmental correlations
    methodology_text = """
    <b>METHODOLOGY</b><br/><br/>
    Sampling was conducted using standardized trawl nets with 20mm mesh size. Environmental parameters 
    including temperature, salinity, dissolved oxygen, pH, and chlorophyll-a concentrations were measured 
    at each station. Statistical analysis employed multivariate ordination techniques to identify 
    environmental drivers of community structure.
    """
    
    story.append(Paragraph(methodology_text, styles['Normal']))
    story.append(Spacer(1, 15))
    
    # Environmental correlation table
    story.append(Paragraph("Table 2: Environmental Variables and Species Correlations", styles['Heading2']))
    
    correlation_data = [
        ['Environmental Variable', 'Range', 'Mean ± SD', 'R² with Diversity', 'P-value', 'Significance'],
        ['Temperature (°C)', '8.2 - 16.8', '12.4 ± 2.1', '0.67', '<0.001', '***'],
        ['Salinity (ppt)', '34.1 - 35.9', '35.2 ± 0.4', '0.43', '0.002', '**'],
        ['Dissolved O₂ (mg/L)', '6.8 - 9.7', '8.2 ± 0.8', '0.51', '<0.001', '***'],
        ['pH', '7.9 - 8.4', '8.1 ± 0.1', '0.29', '0.024', '*'],
        ['Chlorophyll-a (mg/m³)', '0.8 - 4.2', '2.1 ± 0.9', '0.72', '<0.001', '***'],
        ['Depth (m)', '25 - 180', '89 ± 45', '0.38', '0.006', '**']
    ]
    
    corr_table = Table(correlation_data, colWidths=[1.5*inch, 0.9*inch, 1.0*inch, 1.0*inch, 0.8*inch, 0.8*inch])
    corr_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.purple),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lavender),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 7)
    ]))
    
    story.append(corr_table)
    
    doc.build(story)
    print(f"✅ Created marine ecology PDF: {filepath}")
    return filepath

def main():
    """Create all sample PDF files"""
    print("🐠 Creating realistic marine biology PDF samples...")
    
    # Ensure test_files2 directory exists
    os.makedirs("test_files2", exist_ok=True)
    
    # Create different types of PDFs
    pdf_files = []
    
    try:
        # Fish survey PDF
        pdf1 = create_fish_survey_pdf()
        pdf_files.append(pdf1)
        
        # Oceanography data PDF
        pdf2 = create_oceanography_data_pdf()
        pdf_files.append(pdf2)
        
        # Marine ecology PDF
        pdf3 = create_marine_ecology_pdf()
        pdf_files.append(pdf3)
        
        print(f"\n🎉 Successfully created {len(pdf_files)} PDF files:")
        for pdf in pdf_files:
            file_size = os.path.getsize(pdf) / 1024  # Size in KB
            print(f"  📄 {os.path.basename(pdf)} ({file_size:.1f} KB)")
        
        print(f"\n📁 All files saved in: test_files2/")
        print("\nThese PDFs contain:")
        print("  • Realistic marine biology data")
        print("  • Multiple data tables with fish and oceanography data")  
        print("  • Mixed text and tabular content")
        print("  • Scientific formatting and structure")
        print("  • Various table styles and layouts")
        
    except ImportError as e:
        print(f"❌ Error: Missing required package - {e}")
        print("To install required packages, run:")
        print("pip install reportlab")
    except Exception as e:
        print(f"❌ Error creating PDFs: {e}")

if __name__ == "__main__":
    main()