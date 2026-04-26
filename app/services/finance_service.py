# app/services/finance_service.py

def compute_real_total(cash, digital, apps_collected):
    return float(cash or 0.0) + float(digital or 0.0) + float(apps_collected or 0.0)


def compute_pending_net(apps, apps_collected):
    pending_net = 0.0

    if apps is not None:
        pending_net += float(apps)

    if apps_collected is not None:
        pending_net -= float(apps_collected)

    return pending_net


def compute_explained_total(cash, digital, apps, apps_collected):
    total = compute_real_total(cash, digital, apps_collected)
    pending_net = compute_pending_net(apps, apps_collected)

    if total is not None or pending_net != 0:
        return total + pending_net

    return None