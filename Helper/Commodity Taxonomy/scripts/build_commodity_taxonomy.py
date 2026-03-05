#!/usr/bin/env python3
import json
import re
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TXT_DIR = ROOT / "tmp" / "pdfs_txt"
OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _compile_term_pattern(term: str, case_sensitive: bool) -> re.Pattern:
    escaped = re.escape(term.strip())
    escaped = escaped.replace(r"\ ", r"\s+")
    escaped = escaped.replace(r"\-", r"[-\s]?")
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.compile(rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", flags)


_PATTERN_CACHE: dict[tuple[str, bool], re.Pattern] = {}


def contains_term(text: str, term: str) -> bool:
    cleaned = term.strip()
    if not cleaned:
        return False

    compact = re.sub(r"[^A-Za-z0-9]", "", cleaned)
    short_upper = cleaned.isupper() and 1 < len(compact) <= 4

    key = (cleaned, short_upper)
    if key not in _PATTERN_CACHE:
        _PATTERN_CACHE[key] = _compile_term_pattern(cleaned, short_upper)
    return bool(_PATTERN_CACHE[key].search(text))


# Short aliases such as CFR/DAP/GO are useful labels but too ambiguous to
# determine commodity presence on their own.
AMBIGUOUS_SHORT_ALIASES = {
    "AA",
    "ACN",
    "BA",
    "CAM",
    "CAN",
    "CAR",
    "CFR",
    "C2",
    "C3",
    "C4",
    "DAP",
    "DOP",
    "EA",
    "GO",
    "GS",
    "HBI",
    "LNG",
    "MAP",
    "MEA",
    "NPK",
    "PA",
    "PO",
    "REC",
    "RIN",
    "SOP",
    "TEA",
    "TSP",
}


def blocked_for_presence(term: str) -> bool:
    compact = re.sub(r"[^A-Za-z0-9]", "", term)
    if not compact:
        return True
    if term.upper() in AMBIGUOUS_SHORT_ALIASES:
        return True
    if term.isupper() and len(compact) <= 2:
        return True
    return False


SEED = [
    # Chemicals: Olefins and derivatives
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Ethylene",
        "aliases": ["C2"],
        "variants": ["Ethylene Mont Belvieu", "Ethylene Choctaw"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Propylene",
        "aliases": ["C3"],
        "variants": ["Refinery Grade Propylene", "Polymer Grade Propylene", "Chemical Grade Propylene"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Butadiene",
        "aliases": ["C4"],
        "variants": ["Butadiene CP"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Raffinate-1",
        "aliases": ["Raffinate 1"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Butene-1",
        "aliases": ["1-Butene"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Hexene-1",
        "aliases": ["1-Hexene"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Octene-1",
        "aliases": ["1-Octene"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Ethylene Oxide",
        "aliases": ["EO"],
        "variants": ["High Purity Ethylene Oxide"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Propylene Oxide",
        "aliases": ["PO"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Monoethylene Glycol",
        "aliases": ["MEG"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Diethylene Glycol",
        "aliases": ["DEG"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Triethylene Glycol",
        "aliases": ["TEG"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Monoethanolamine",
        "aliases": ["MEA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Diethanolamine",
        "aliases": ["DEA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Olefins and Derivatives",
        "name": "Triethanolamine",
        "aliases": ["TEA"],
        "variants": [],
    },

    # Chemicals: Aromatics and derivatives
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Benzene",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Toluene",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Mixed Xylenes",
        "aliases": ["MX", "Mixed xylenes"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Paraxylene",
        "aliases": ["PX"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Orthoxylene",
        "aliases": ["OX"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Ethylbenzene",
        "aliases": ["EB"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Styrene Monomer",
        "aliases": ["SM", "Styrene monomer"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Caprolactam",
        "aliases": ["CPL"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Cyclohexane",
        "aliases": ["CX"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Adipic Acid",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Aromatics and Derivatives",
        "name": "Cumene",
        "aliases": [],
        "variants": [],
    },

    # Chemicals: Methanol and ethers
    {
        "category": "Chemicals",
        "subcategory": "Methanol and Ethers",
        "name": "Methanol",
        "aliases": [],
        "variants": ["Methanol Marine Fuel", "MMF", "Methanol Bunker"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Methanol and Ethers",
        "name": "MTBE",
        "aliases": ["Methyl Tertiary Butyl Ether"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Methanol and Ethers",
        "name": "ETBE",
        "aliases": ["Ethyl Tertiary Butyl Ether"],
        "variants": [],
    },

    # Chemicals: Polymers and elastomers
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "High Density Polyethylene",
        "aliases": ["HDPE"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Low Density Polyethylene",
        "aliases": ["LDPE"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Linear Low Density Polyethylene",
        "aliases": ["LLDPE"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polyethylene",
        "aliases": ["PE"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polypropylene",
        "aliases": ["PP"],
        "variants": ["PP Inflated Film", "BOPP"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polyvinyl Chloride",
        "aliases": ["PVC"],
        "variants": ["Suspension PVC"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Ethylene Vinyl Acetate",
        "aliases": ["EVA"],
        "variants": ["M-EVA", "H-EVA"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polystyrene",
        "aliases": ["PS"],
        "variants": ["GPPS", "HIPS", "EPS"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Acrylonitrile Butadiene Styrene",
        "aliases": ["ABS"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polyethylene Terephthalate",
        "aliases": ["PET", "Polyethylene Terephtalate"],
        "variants": ["Food-Grade Pellets"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polyoxymethylene",
        "aliases": ["POM"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polymethyl Methacrylate",
        "aliases": ["PMMA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polycarbonate",
        "aliases": ["PC"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polybutylene Terephthalate",
        "aliases": ["PBT"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Nylon",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Acrylic Fiber",
        "aliases": ["Acrylic fibers"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Epoxy Resin",
        "aliases": ["Epoxy Resins"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Styrene Butadiene Rubber",
        "aliases": ["SBR"],
        "variants": ["ESBR", "SSBR"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Polybutadiene Rubber",
        "aliases": ["PBR"],
        "variants": ["H-PBR"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Polymers and Elastomers",
        "name": "Natural Rubber",
        "aliases": [],
        "variants": [],
    },

    # Chemicals: Intermediates and solvents
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Acetic Acid",
        "aliases": ["AA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Vinyl Acetate Monomer",
        "aliases": ["VAM"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Acrylonitrile",
        "aliases": ["ACN"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Ethylene Dichloride",
        "aliases": ["EDC"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Vinyl Chloride Monomer",
        "aliases": ["VCM"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Purified Terephthalic Acid",
        "aliases": ["PTA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Carbon Black",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Acrylic Acid",
        "aliases": ["GAA", "Glacial Acrylic Acid"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Butyl Acrylate",
        "aliases": ["BA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Methyl Methacrylate",
        "aliases": ["MMA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Bisphenol-A",
        "aliases": ["BPA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Polyols",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Isocyanates",
        "aliases": [],
        "variants": ["TDI", "Toluene Diisocyanate"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Phthalic Anhydride",
        "aliases": ["PA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Dioctyl Phthalate",
        "aliases": ["DOP"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Diisononyl Phthalate",
        "aliases": ["DINP"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Dioctyl Terephthalate",
        "aliases": ["DOTP"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Hexane",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Isopropyl Alcohol",
        "aliases": ["IPA"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Phenol",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Acetone",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Ethyl Acetate",
        "aliases": ["Etac", "Ethyl acetate"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "N-Butyl Acetate",
        "aliases": ["Butac", "N-butyl acetate"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Methyl Ethyl Ketone",
        "aliases": ["MEK"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Normal Butanol",
        "aliases": ["NBA", "N-butanol"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "Iso-butanol",
        "aliases": ["IBA", "Iso-butanol"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Intermediates and Solvents",
        "name": "2-Ethyl Hexanol",
        "aliases": ["2-EH", "Ethyl Hexanol"],
        "variants": [],
    },

    # Chemicals: Chlor-alkali
    {
        "category": "Chemicals",
        "subcategory": "Chlor-Alkali",
        "name": "Chlorine",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Chlor-Alkali",
        "name": "Caustic Soda",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Chlor-Alkali",
        "name": "Hydrochloric Acid",
        "aliases": [],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Chlor-Alkali",
        "name": "Soda Ash",
        "aliases": [],
        "variants": [],
    },

    # Chemicals: Recycled polymers and waste feedstocks
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "PET Bottle Bales",
        "aliases": ["Post-Consumer PET Bottle Bales"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "Recycled PET Flakes",
        "aliases": ["R-PET Flakes"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "Recycled PET Food-Grade Pellets",
        "aliases": ["R-PET Food-Grade Pellets"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "HDPE Bales",
        "aliases": ["Post-Consumer HDPE Bales"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "Recycled HDPE Pellets",
        "aliases": ["R-HDPE"],
        "variants": ["R-HDPE Black"],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "LDPE Bales",
        "aliases": ["Post-use LDPE Bales"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "Recycled LDPE Pellets",
        "aliases": ["R-LDPE"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "Recycled Polypropylene",
        "aliases": ["R-PP"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "Recycled Polystyrene",
        "aliases": ["R-PS"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "Recycled ABS",
        "aliases": ["R-ABS"],
        "variants": [],
    },
    {
        "category": "Chemicals",
        "subcategory": "Recycled Polymers and Waste Feedstocks",
        "name": "Mixed Plastic Waste Bales",
        "aliases": ["Process-ready Mixed Plastic Waste Bales"],
        "variants": [],
    },

    # Energy: Crude Oil
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Brent", "aliases": [], "variants": ["Brent Strip"]},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Dated Brent", "aliases": [], "variants": ["Dated North Sea Light", "Dated Brent CIF Rotterdam"]},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Dubai Crude", "aliases": ["Dubai"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Oman Crude", "aliases": ["Oman"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Murban", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Upper Zakum", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Das Blend", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Arab Light", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Al-Shaheen", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Forties", "aliases": [], "variants": ["Forties Blend"]},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Oseberg", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Ekofisk", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Troll", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Statfjord", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Gullfaks", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Brent/Ninian Blend", "aliases": ["BNB"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "West Texas Intermediate", "aliases": ["WTI"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "WTI Midland", "aliases": [], "variants": ["MEH", "West Texas Intermediate (MEH)"]},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Light Louisiana Sweet", "aliases": ["LLS"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Heavy Louisiana Sweet", "aliases": ["HLS"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Mars Crude", "aliases": ["Mars"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Southern Green Canyon", "aliases": ["SGC"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Alaska North Slope", "aliases": ["ANS"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Western Canadian Select", "aliases": ["WCS"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Urals", "aliases": [], "variants": ["Urals Netbacks", "Urals FIP Surgut netback"]},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "CPC Blend", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Azeri Light", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Saharan Blend", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "ESPO", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Bonny Light", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Forcados", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Agbami", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Qua Iboe", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Cabinda", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Dalia", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Bonga", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Hungo", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Girassol", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Djeno", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Napo", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Oriente", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Vasconia", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Maya", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Isthmus", "aliases": ["Istmo"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Olmeca", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Castilla Blend", "aliases": ["Castilla"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Merey 16", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Kimanis", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Cinta", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Widuri", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Minas", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Nile Blend", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Medanito", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Rubiales", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Roncador Heavy", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Tupi", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Liza", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Payara", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Unity Gold", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Galeota Mix", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Escalante", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Canadon Seco", "aliases": ["Canadon Seco"], "variants": []},
    {"category": "Energy", "subcategory": "Crude Oil", "name": "Cuban Heavy", "aliases": [], "variants": []},

    # Energy: Refined products
    {"category": "Energy", "subcategory": "Refined Products", "name": "LPG", "aliases": [], "variants": ["NGLs", "LPG/NGLs"]},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Propane", "aliases": [], "variants": ["Propane CP"]},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Butane", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Naphtha", "aliases": ["Light Naphtha"], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Gasoline", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "RBOB Gasoline", "aliases": ["RBOB"], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Eurobob Gasoline", "aliases": ["Eurobob"], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Jet Fuel", "aliases": ["Jet"], "variants": ["World Jet Indexes"]},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Kerosene", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Diesel", "aliases": [], "variants": ["10ppm diesel"]},
    {"category": "Energy", "subcategory": "Refined Products", "name": "ULSD", "aliases": ["Ultra Low Sulfur Diesel"], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Gasoil", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Heating Oil", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Fuel Oil", "aliases": [], "variants": ["A-FUEL OIL"]},
    {"category": "Energy", "subcategory": "Refined Products", "name": "High Sulfur Fuel Oil", "aliases": ["HSFO", "IFO 380"], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Very Low Sulfur Fuel Oil", "aliases": ["VLSFO", "Fuel 0.5%"], "variants": ["IFO 500"]},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Marine Gasoil", "aliases": ["MGO"], "variants": ["DMA 0.1%"]},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Marine Diesel Oil", "aliases": ["MDO"], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Vacuum Gas Oil", "aliases": ["VGO"], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Straight Run Fuel Oil", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Bitumen", "aliases": [], "variants": ["Asphalt"]},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Base Oil", "aliases": ["Base Oils"], "variants": []},
    {"category": "Energy", "subcategory": "Refined Products", "name": "Blendstocks", "aliases": [], "variants": []},

    # Energy: Natural gas and LNG
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Natural Gas", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Liquefied Natural Gas", "aliases": ["LNG"], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Bio-LNG", "aliases": [], "variants": ["Subsidized Bio-LNG"]},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Biomethane", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Renewable Natural Gas", "aliases": ["RNG"], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Japan Korea Marker", "aliases": ["JKM"], "variants": ["JKTC"]},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Dutch TTF", "aliases": ["TTF"], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "National Balancing Point", "aliases": ["NBP"], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Zeebrugge Trading Point", "aliases": ["ZTP"], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Henry Hub", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "AECO", "aliases": ["AECO-C"], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Waha", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Algonquin City-Gates", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Dawn Ontario", "aliases": ["Dawn"], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Eastern Gas South", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Houston Ship Channel Gas", "aliases": ["Houston Ship Channel"], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "NGPL Midcontinent", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "PG&E Malin", "aliases": ["PG&E Malin"], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "SoCal Gas", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Natural Gas and LNG", "name": "Transco Zone 6 Non-NY", "aliases": [], "variants": []},

    # Energy: Coal and coke
    {"category": "Energy", "subcategory": "Coal and Coke", "name": "Thermal Coal", "aliases": [], "variants": ["CAPP rail"]},
    {"category": "Energy", "subcategory": "Coal and Coke", "name": "Metallurgical Coal", "aliases": ["Met Coal", "Metcoal"], "variants": []},
    {"category": "Energy", "subcategory": "Coal and Coke", "name": "Hard Coking Coal", "aliases": ["HCC"], "variants": ["PLV HCC", "Low Vol HCC"]},
    {"category": "Energy", "subcategory": "Coal and Coke", "name": "Semi-Soft Coking Coal", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Coal and Coke", "name": "Pulverized Coal Injection Coal", "aliases": ["PCI"], "variants": []},
    {"category": "Energy", "subcategory": "Coal and Coke", "name": "Metallurgical Coke", "aliases": ["Met Coke"], "variants": []},
    {"category": "Energy", "subcategory": "Coal and Coke", "name": "Petroleum Coke", "aliases": ["Petcoke"], "variants": ["Calcined Petcoke"]},

    # Energy: Power
    {"category": "Energy", "subcategory": "Power", "name": "Electricity", "aliases": [], "variants": ["Renewable Capture Price Indices", "BESS Indices"]},

    # Energy: Biofuels
    {"category": "Energy", "subcategory": "Biofuels", "name": "Ethanol", "aliases": ["ENA", "REN 96"], "variants": ["Anhydrous Ethanol", "Ethanol T2"]},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Biodiesel", "aliases": ["FAME"], "variants": ["B100"]},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Soy Methyl Ester", "aliases": ["SME"], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Rapeseed Methyl Ester", "aliases": ["RME"], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Palm Methyl Ester", "aliases": ["PME"], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Used Cooking Oil Methyl Ester", "aliases": ["UCOME"], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Renewable Diesel", "aliases": ["RD"], "variants": ["R99", "R100"]},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Hydrotreated Vegetable Oil", "aliases": ["HVO"], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Sustainable Aviation Fuel", "aliases": ["SAF"], "variants": ["HEFA-SPK", "SPK-HEFA"]},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Biomass-Based Diesel", "aliases": ["BBD"], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Distillers Corn Oil", "aliases": [], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Used Cooking Oil", "aliases": ["UCO"], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Palm Fatty Acid Distillate", "aliases": ["PFAD"], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Palm Oil Mill Effluent", "aliases": ["POME"], "variants": []},
    {"category": "Energy", "subcategory": "Biofuels", "name": "Biobunker Fuel", "aliases": ["Biobunkers"], "variants": []},

    # Energy: Hydrogen and ammonia
    {"category": "Energy", "subcategory": "Hydrogen and Ammonia", "name": "Hydrogen", "aliases": [], "variants": ["Renewable PPA Derived Hydrogen"]},
    {"category": "Energy", "subcategory": "Hydrogen and Ammonia", "name": "Ammonia", "aliases": [], "variants": ["Japan Korea Ammonia Price", "US Gulf Ammonia Price"]},
    {"category": "Energy", "subcategory": "Hydrogen and Ammonia", "name": "Low-Carbon Ammonia", "aliases": ["Low Carbon Ammonia"], "variants": ["AESI"]},

    # Metals: Ferrous
    {"category": "Metals", "subcategory": "Ferrous", "name": "Iron Ore", "aliases": ["IODEX"], "variants": ["Carbon-Accounted Iron Ore"]},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Iron Ore Fines", "aliases": ["Fines"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Iron Ore Lump", "aliases": ["Lump"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Iron Ore Pellets", "aliases": ["Pellet"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Pellet Feed", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Iron Ore Concentrate", "aliases": ["Domestic Concentrate"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Direct Reduced Iron", "aliases": ["DRI"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Hot Briquetted Iron", "aliases": ["HBI"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Hot-Rolled Coil", "aliases": ["HRC"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Cold-Rolled Coil", "aliases": ["CRC"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Steel Plate", "aliases": ["PLATE"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Rebar", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Wire Rod", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Steel Billet", "aliases": ["Billet"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Steel Slab", "aliases": ["Slab"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Coated Steel", "aliases": ["COATED"], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Ferrous Scrap", "aliases": ["Scrap"], "variants": ["Shredded scrap"]},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Ferroalloys", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Stainless Steel", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Ferrous", "name": "Metallics", "aliases": [], "variants": []},

    # Metals: Nonferrous
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Alumina", "aliases": ["SGA"], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Aluminum", "aliases": ["Aluminium"], "variants": ["P1020"]},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Aluminum Alloy", "aliases": ["ADC12"], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Copper", "aliases": [], "variants": ["Copper Cathode"]},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Copper Concentrate", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Copper Scrap", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Lead", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Lead Scrap", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Zinc", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Zinc Secondary Alloys", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Tin", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Nickel", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Nickel Pig Iron", "aliases": ["NPI"], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Lithium", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Cobalt", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Manganese", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Graphite", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Cathode Active Material", "aliases": ["CAM"], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Magnesium", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Silicon", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Nonferrous", "name": "Titanium", "aliases": [], "variants": []},

    # Metals: Precious and nuclear
    {"category": "Metals", "subcategory": "Precious and Nuclear", "name": "Platinum", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Precious and Nuclear", "name": "Rhodium", "aliases": [], "variants": []},
    {"category": "Metals", "subcategory": "Precious and Nuclear", "name": "Uranium", "aliases": ["U3O8"], "variants": []},

    # Agriculture: Grains and oilseeds
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Rice", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Wheat", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Corn", "aliases": ["Maize"], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Soybeans", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Soybean Meal", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Soybean Oil", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Vegetable Oils", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Crude Palm Oil", "aliases": ["CPO"], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Palm Kernel Oil", "aliases": ["CPKO"], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Sunflower Oil", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Grains and Oilseeds", "name": "Canola", "aliases": ["Rapeseed"], "variants": []},

    # Agriculture: Sugar
    {"category": "Agriculture", "subcategory": "Sugar", "name": "Raw Sugar", "aliases": [], "variants": ["Thai Hi Polarization", "Brazil 150 ICUMSA"]},
    {"category": "Agriculture", "subcategory": "Sugar", "name": "White Sugar", "aliases": [], "variants": ["Europe 45 ICUMSA", "White Sugar Premium"]},

    # Agriculture: Fertilizers
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Urea", "aliases": [], "variants": ["Prilled Urea", "Granular Urea", "Automotive-grade Urea"]},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "UAN Solution", "aliases": ["UAN"], "variants": ["32% N UAN Solution"]},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Ammonium Nitrate", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Calcium Ammonium Nitrate", "aliases": ["CAN"], "variants": []},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Ammonium Sulfate", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Ammonium Thiosulfate", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Diammonium Phosphate", "aliases": ["DAP"], "variants": ["18-46-0"]},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Monoammonium Phosphate", "aliases": ["MAP"], "variants": ["12-52-0", "11-52-0"]},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Triple Superphosphate", "aliases": ["TSP", "GTSP"], "variants": ["0-46-0"]},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Phosphate Rock", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "NPK Fertilizer", "aliases": ["NPK"], "variants": []},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Potash", "aliases": [], "variants": ["KCl", "K2SO4", "SOP"]},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Sulfur", "aliases": ["Solid Sulfur", "Liquid Sulfur"], "variants": []},
    {"category": "Agriculture", "subcategory": "Fertilizers", "name": "Sulfuric Acid", "aliases": [], "variants": []},

    # Agriculture: Proteins and feed
    {"category": "Agriculture", "subcategory": "Proteins and Feed", "name": "Beef", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Proteins and Feed", "name": "Pork", "aliases": [], "variants": []},
    {"category": "Agriculture", "subcategory": "Proteins and Feed", "name": "Poultry", "aliases": ["Chicken"], "variants": []},
    {"category": "Agriculture", "subcategory": "Proteins and Feed", "name": "Seafood", "aliases": [], "variants": ["Shrimp"]},
    {"category": "Agriculture", "subcategory": "Proteins and Feed", "name": "Animal Feed", "aliases": [], "variants": ["DDGS", "DDG"]},

    # Environmental markets: Carbon credits and allowances
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "Avoidance Carbon Credits", "aliases": ["Avoidance Credits"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "Removals Carbon Credits", "aliases": ["Removals Credits"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "CORSIA Eligible Credit", "aliases": ["CEC"], "variants": ["Pre-CEC"]},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "EU Allowances", "aliases": ["EUA"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "UK Allowances", "aliases": ["UKA"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "California Carbon Allowances", "aliases": ["CCA"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "RGGI Allowances", "aliases": ["RGGI"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "New Zealand Unit", "aliases": ["NZU"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "Korean Allowance Unit", "aliases": ["KAU"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "Korean Offset Credit", "aliases": ["KOC"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "International Carbon Credit", "aliases": ["ICC"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "Australian Carbon Credit Unit", "aliases": ["ACCU"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "Verified Carbon Standard Credits", "aliases": ["VCS"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "Gold Standard Credits", "aliases": ["GS"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Carbon Credits and Allowances", "name": "Climate Action Reserve Credits", "aliases": ["CAR"], "variants": []},

    # Environmental markets: Renewable certificates and fuel credits
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "Renewable Energy Certificate", "aliases": ["REC"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "Guarantees of Origin", "aliases": ["GO"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "Renewable Gas Guarantees of Origin", "aliases": ["RGGO"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "HBE Advanced", "aliases": ["HBE-A"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "HBE Annex IX B", "aliases": ["HBE-B"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "HBE Conventional", "aliases": ["HBE-C"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "HBE Other", "aliases": ["HBE-O"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "THG Conventional", "aliases": ["THG-Conventional"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "THG Other", "aliases": ["THG-Other"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "THG Annex IX B", "aliases": ["THG-Annex IX B"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "Renewable Identification Number", "aliases": ["RIN"], "variants": ["D3", "D4", "D5", "D6"]},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "Low Carbon Fuel Standard Credits", "aliases": ["LCFS"], "variants": ["BC LCFS"]},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "Washington Clean Fuel Standard Credits", "aliases": ["WACFS"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "Canada Clean Fuel Regulation Credits", "aliases": ["CFR"], "variants": []},
    {"category": "Environmental Markets", "subcategory": "Renewable Certificates and Fuel Credits", "name": "Oregon Clean Fuel Program Credits", "aliases": ["OCFP"], "variants": []},

    # Construction materials
    {"category": "Construction Materials", "subcategory": "Cement and Clinker", "name": "Cement", "aliases": [], "variants": ["Type I / II Low Alkali"]},
    {"category": "Construction Materials", "subcategory": "Cement and Clinker", "name": "Clinker", "aliases": [], "variants": []},
    {"category": "Construction Materials", "subcategory": "Cement and Clinker", "name": "Limestone", "aliases": [], "variants": []},
]


AMBIGUOUS_RULES = [
    {
        "term": "Ammonia",
        "note": "Can be fuel/energy carrier or fertilizer input. Kept in Energy > Hydrogen and Ammonia; also appears in fertilizer contexts.",
        "chosen_category": "Energy > Hydrogen and Ammonia",
    },
    {
        "term": "Naphtha",
        "note": "Can be refined product and petrochemical feedstock. Kept in Energy > Refined Products.",
        "chosen_category": "Energy > Refined Products",
    },
    {
        "term": "Methanol",
        "note": "Appears as chemical and marine fuel. Kept in Chemicals > Methanol and Ethers with MMF variant.",
        "chosen_category": "Chemicals > Methanol and Ethers",
    },
    {
        "term": "Sulfur",
        "note": "Appears as fertilizer commodity and crude/refined quality parameter. Kept in Agriculture > Fertilizers.",
        "chosen_category": "Agriculture > Fertilizers",
    },
    {
        "term": "Coke",
        "note": "Used for both petroleum coke and metallurgical coke. Split into two commodities under Energy > Coal and Coke.",
        "chosen_category": "Energy > Coal and Coke",
    },
    {
        "term": "Fuel Oil",
        "note": "Generic label can overlap HSFO/VLSFO marine bunker products. Kept as parent commodity with separate HSFO/VLSFO entries.",
        "chosen_category": "Energy > Refined Products",
    },
    {
        "term": "Freight",
        "note": "Freight rates are not physical commodities; excluded from categories and flagged for review if needed for headline routing.",
        "chosen_category": "Excluded (non-commodity market service)",
    },
    {
        "term": "Carbon Intensity",
        "note": "Carbon intensity is a metric rather than a tradable commodity. Related instruments are included under Environmental Markets.",
        "chosen_category": "Excluded metric; related credits in Environmental Markets",
    },
    {
        "term": "Electricity",
        "note": "Electricity appears as power index/assessment products rather than physical inventories. Kept in Energy > Power for taxonomy utility.",
        "chosen_category": "Energy > Power",
    },
]


def load_pdf_texts() -> dict[str, str]:
    pdf_files = sorted(path.name for path in ROOT.glob("*.pdf"))
    texts: dict[str, str] = {}
    missing = []

    for pdf in pdf_files:
        txt_file = TXT_DIR / f"{Path(pdf).stem}.txt"
        if not txt_file.exists():
            missing.append(str(txt_file))
            continue
        texts[pdf] = txt_file.read_text(errors="ignore")

    if missing:
        raise FileNotFoundError("Missing extracted text files:\n" + "\n".join(missing))

    return texts


def match_entry_to_sources(entry: dict, pdf_texts: dict[str, str]) -> dict | None:
    name = entry["name"]
    aliases = entry.get("aliases", [])
    variants = entry.get("variants", [])

    matched_aliases: set[str] = set()
    matched_variants: set[str] = set()
    source_pdfs: list[str] = []

    search_terms = [name] + aliases + variants
    primary_terms = [term for term in search_terms if not blocked_for_presence(term)]
    if name not in primary_terms:
        primary_terms.insert(0, name)

    for pdf, text in pdf_texts.items():
        found_in_pdf = False
        for term in primary_terms:
            if contains_term(text, term):
                found_in_pdf = True

        if found_in_pdf:
            for alias in aliases:
                if contains_term(text, alias):
                    matched_aliases.add(alias)
            for variant in variants:
                if contains_term(text, variant):
                    matched_variants.add(variant)

        if found_in_pdf:
            source_pdfs.append(pdf)

    if not source_pdfs:
        return None

    return {
        "category": entry["category"],
        "subcategory": entry["subcategory"],
        "name": name,
        "aliases": sorted(matched_aliases),
        "variants": sorted(matched_variants),
        "source_pdfs": source_pdfs,
    }


def build_taxonomy(found_entries: list[dict]) -> dict:
    categories_map: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for item in found_entries:
        categories_map[item["category"]][item["subcategory"]].append(
            {
                "name": item["name"],
                "aliases": item["aliases"],
                "variants": item["variants"],
                "source_pdfs": item["source_pdfs"],
            }
        )

    categories = []
    for category_name in sorted(categories_map.keys()):
        subcategories = []
        for sub_name in sorted(categories_map[category_name].keys()):
            commodities = sorted(categories_map[category_name][sub_name], key=lambda x: x["name"].lower())
            subcategories.append({"name": sub_name, "commodities": commodities})
        categories.append({"name": category_name, "subcategories": subcategories})

    return {"categories": categories}


def build_duplicates(found_entries: list[dict], pdf_texts: dict[str, str]) -> list[dict]:
    merged = []
    seen = set()

    for item in found_entries:
        canonical = item["name"]
        for alias in item["aliases"] + item["variants"]:
            if alias.lower() == canonical.lower():
                continue
            # Ignore ultra-short acronym-only variants in duplicates list.
            compact = re.sub(r"[^A-Za-z0-9]", "", alias)
            if alias.isupper() and len(compact) <= 4:
                continue

            # Require alias to appear in at least one source file.
            appears = any(contains_term(pdf_texts[pdf], alias) for pdf in item["source_pdfs"])
            if not appears:
                continue

            key = (alias.lower(), canonical.lower())
            if key in seen:
                continue
            seen.add(key)
            merged.append({"from": alias, "to": canonical})

    return sorted(merged, key=lambda x: (x["to"].lower(), x["from"].lower()))


def build_needs_review(pdf_texts: dict[str, str]) -> list[dict]:
    needs_review = []

    for rule in AMBIGUOUS_RULES:
        term = rule["term"]
        sources = [pdf for pdf, text in pdf_texts.items() if contains_term(text, term)]
        if not sources:
            continue
        needs_review.append(
            {
                "term": term,
                "reason": rule["note"],
                "chosen_category": rule["chosen_category"],
                "source_pdfs": sources,
            }
        )

    return needs_review


def build_processing_log(found_entries: list[dict], ordered_pdfs: list[str]) -> list[dict]:
    first_seen = {}
    for item in found_entries:
        first_seen[item["name"]] = item["source_pdfs"][0]

    by_pdf = defaultdict(list)
    for commodity, pdf in first_seen.items():
        by_pdf[pdf].append(commodity)

    log = []
    for pdf in ordered_pdfs:
        additions = sorted(by_pdf.get(pdf, []), key=str.lower)
        log.append(
            {
                "pdf": pdf,
                "new_commodities_introduced": additions,
                "new_count": len(additions),
            }
        )

    return log


def main() -> None:
    pdf_texts = load_pdf_texts()
    ordered_pdfs = sorted(pdf_texts.keys())

    matched_entries = []
    seen_names = set()

    for entry in SEED:
        matched = match_entry_to_sources(entry, pdf_texts)
        if not matched:
            continue

        # Ensure one canonical entry per commodity name.
        key = matched["name"].lower()
        if key in seen_names:
            continue
        seen_names.add(key)
        matched_entries.append(matched)

    matched_entries = sorted(matched_entries, key=lambda x: x["name"].lower())

    taxonomy = build_taxonomy(matched_entries)
    needs_review = build_needs_review(pdf_texts)
    taxonomy["needs_review"] = needs_review

    duplicates_merged = build_duplicates(matched_entries, pdf_texts)
    processing_log = build_processing_log(matched_entries, ordered_pdfs)

    report = {
        "total_pdfs_processed": len(ordered_pdfs),
        "total_commodities": len(matched_entries),
        "duplicates_merged": duplicates_merged,
        "processing_log": processing_log,
        "needs_review": needs_review,
    }

    (OUTPUT_DIR / "commodity_taxonomy.json").write_text(
        json.dumps(taxonomy, indent=2, ensure_ascii=True) + "\n"
    )
    (OUTPUT_DIR / "commodity_taxonomy_report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=True) + "\n"
    )

    print(f"Processed {len(ordered_pdfs)} PDFs")
    print(f"Matched commodities: {len(matched_entries)}")
    print(f"Duplicates merged: {len(duplicates_merged)}")
    print(f"Needs review terms: {len(needs_review)}")


if __name__ == "__main__":
    main()
