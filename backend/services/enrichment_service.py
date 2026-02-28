def enrich_site(site_class: str, site_flag: str) -> dict:
    cls = site_class
    flag = site_flag

    if flag == "No BTS":
        site_category = "Non-Komersial"
    elif flag == "Site Reguler":
        site_category = "Komersial"
    else:
        site_category = "Non-Komersial"

    if flag == "No BTS":
        site_sub_class = "No BTS"
    elif flag == "Site Reguler":
        site_sub_class = cls
    elif flag == "3T":
        site_sub_class = f"{cls}-3T"
    elif flag == "USO/MP":
        site_sub_class = f"{cls}-USO"
    elif flag == "Femto":
        site_sub_class = f"{cls}-Femto"
    else:
        site_sub_class = cls

    if flag == "No BTS":
        est_technology = "Tidak Ada BTS"
    elif cls in ("Diamond", "Platinum"):
        est_technology = "Multi (2G/3G/4G/5G)"
    elif cls in ("Gold", "Silver"):
        est_technology = "Multi (2G/3G/4G)"
    elif cls == "Bronze" and flag == "Site Reguler":
        est_technology = "Limited (4G / 2G+4G)"
    elif cls == "Bronze" and flag in ("3T", "USO/MP"):
        est_technology = "Single (4G LTE)"
    elif cls == "Bronze" and flag == "Femto":
        est_technology = "Single (4G Femto)"
    else:
        est_technology = "N/A"

    if flag == "No BTS":
        est_transmission = "Tidak Ada"
    elif cls in ("Diamond", "Platinum"):
        est_transmission = "Fiber Optic + Microwave"
    elif cls in ("Gold", "Silver"):
        est_transmission = "Fiber Optic atau Microwave"
    elif cls == "Bronze" and flag == "Site Reguler":
        est_transmission = "Microwave"
    elif cls == "Bronze" and flag in ("3T", "USO/MP"):
        est_transmission = "VSAT atau Microwave"
    elif cls == "Bronze" and flag == "Femto":
        est_transmission = "WiFi Backhaul / VSAT"
    else:
        est_transmission = "N/A"

    if flag == "No BTS":
        est_power = "Tidak Ada"
    elif cls in ("Diamond", "Platinum"):
        est_power = "PLN + Genset + Baterai (redundan)"
    elif cls in ("Gold", "Silver"):
        est_power = "PLN + Baterai"
    elif cls == "Bronze" and flag == "Site Reguler":
        est_power = "PLN + Baterai (minimal)"
    elif cls == "Bronze" and flag in ("3T", "USO/MP"):
        est_power = "Solar Panel + Baterai"
    elif cls == "Bronze" and flag == "Femto":
        est_power = "PLN (rumah/gedung)"
    else:
        est_power = "N/A"

    if flag == "No BTS":
        est_sector = "Tidak Ada"
    elif cls in ("Diamond", "Platinum"):
        est_sector = "3+ Sektor (directional)"
    elif cls in ("Gold", "Silver"):
        est_sector = "3 Sektor"
    elif cls == "Bronze" and flag == "Site Reguler":
        est_sector = "1-2 Sektor"
    elif cls == "Bronze" and flag in ("3T", "USO/MP", "Femto"):
        est_sector = "Omni (360°)"
    else:
        est_sector = "N/A"

    if flag == "No BTS":
        complexity_level = 0
    elif cls in ("Diamond", "Platinum"):
        complexity_level = 5
    elif cls in ("Gold", "Silver"):
        complexity_level = 4
    elif cls == "Bronze" and flag == "Site Reguler":
        complexity_level = 3
    elif cls == "Bronze" and flag in ("3T", "USO/MP"):
        complexity_level = 2
    elif cls == "Bronze" and flag == "Femto":
        complexity_level = 1
    else:
        complexity_level = 0

    opex_map = {5: "Very High", 4: "High", 3: "Medium", 2: "Low", 1: "Very Low", 0: "N/A"}
    est_opex_level = opex_map.get(complexity_level, "N/A")

    if cls in ("Diamond", "Platinum"):
        upgrade_potential = "N/A (Tertinggi)"
    elif cls == "Gold":
        upgrade_potential = "Low"
    elif cls == "Silver":
        upgrade_potential = "Medium"
    elif cls == "Bronze":
        upgrade_potential = "High"
    else:
        upgrade_potential = "N/A"

    if flag == "No BTS":
        strategy_focus = "Non-Applicable"
    elif cls in ("Diamond", "Platinum"):
        strategy_focus = "Capacity & Quality Management"
    elif cls in ("Gold", "Silver"):
        strategy_focus = "Reliability & Optimization"
    elif cls == "Bronze" and flag == "Site Reguler":
        strategy_focus = "OPEX Efficiency"
    elif cls == "Bronze" and flag in ("3T", "USO/MP"):
        strategy_focus = "Availability & Access"
    elif cls == "Bronze" and flag == "Femto":
        strategy_focus = "Monitoring Minimal"
    else:
        strategy_focus = "N/A"

    return {
        "site_category": site_category,
        "site_sub_class": site_sub_class,
        "est_technology": est_technology,
        "est_transmission": est_transmission,
        "est_power": est_power,
        "est_sector": est_sector,
        "complexity_level": complexity_level,
        "est_opex_level": est_opex_level,
        "upgrade_potential": upgrade_potential,
        "strategy_focus": strategy_focus,
    }
