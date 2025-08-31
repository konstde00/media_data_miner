PAIN_MINING_SYSTEM = (
    "You are an analyst extracting recurring user pains and unmet needs from Reddit snippets. "
    "Prefer concrete problems over vague complaints."
)

PAIN_MINING_USER = (
    "Given these snippets about {topic} (subreddits: {subs}, period: {period}), list the top pains as\n"
    "(pain -> why it's painful -> who feels it -> 1-2 short proof quotes with permalinks).\n"
    "Rules:\n1) Use only the provided snippets.\n2) Merge near-duplicates.\n3) Output 5-10 pains ranked by severity x frequency.\n\n"
    "Snippets:\n{snippets}"
)

IDEA_GEN_USER = (
    "Propose {n} Micro-SaaS ideas that directly solve the top pains. For each idea:\n"
    "- One-line value prop\n- Core feature set (must-have only)\n- MVP in 1 week (exact steps)\n"
    "- Who pays and why now\n- Moat angle (data, workflow lock-in, distribution)\n- Risks & obvious killers\n"
    "Use only the information consistent with the snippets below; avoid generic platitudes.\n\n"
    "Snippets:\n{snippets}"
)

SCORING_USER = (
    "Score each idea (0-5) on: Pain severity, Urgency, Frequency, Willingness-to-pay, Ease of building, "
    "Distribution advantage, Competitive crowding (reverse-scored). Return a table sorted by (Pain x WTP x Urgency) - Competition with one-sentence justification per score.\n\n"
    "Ideas:\n{ideas}"
)