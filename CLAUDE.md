# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repository Is

A Python/Jupyter notebook educational repository for machine learning coursework. Notebooks cover classification (LDA, Logistic Regression, SVM), manual gradient descent, model evaluation metrics, and linear regression with feature engineering. All work is exploratory/interactive — there is no build system, package manager, CI/CD, or test suite.

## Running Notebooks

```bash
jupyter notebook          # launch Jupyter in browser
jupyter nbconvert --to notebook --execute <notebook.ipynb>  # run a notebook headlessly
```

There are no lint, test, or build commands.

## Repository Layout

| Directory | Content |
|-----------|---------|
| `FE1/` | Binary classification on the Adult income dataset (LDA, LogReg, SVM via sklearn pipelines) |
| `FE2/` | Logistic regression implemented from scratch with manual gradient descent |
| `Lection3/` | Model evaluation: ROC/AUC, precision-recall curves, cross-validation, L1/L2 regularization |
| `Lection5/` | Linear regression with feature engineering (composite features, polynomial terms) |

CSV data files live alongside each notebook inside the same directory.

## Standard Patterns Used Across Notebooks

**Imports** — every notebook uses this core stack:
```python
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.pipeline import make_pipeline
from sklearn.metrics import accuracy_score, roc_curve, auc
import matplotlib.pyplot as plt
```

**Preprocessing** — categorical columns are encoded via `LabelEncoder` or `pd.get_dummies()` before splitting; splits are typically 70/30 or 80/20.

**Model training** — models are wrapped in `make_pipeline(StandardScaler(), Model())` so scaling is always part of the estimator.

**Evaluation** — notebooks compute accuracy, precision, recall, F1, confusion matrix entries (TP/FP/TN/FN), ROC-AUC, and sometimes PR-AUC inline in cells; there is no separate evaluation script.

## Key Conventions

- No `requirements.txt` — dependencies (pandas, numpy, scikit-learn, matplotlib, seaborn) are assumed pre-installed in the Jupyter environment.
- Notebooks may be named `Untitled.ipynb`; context comes from the directory name.
- Feature engineering creates composite variables directly in DataFrame columns before fitting (e.g., `df['mult'] = df['length'] * df['width']`).
- Russian-language filenames/comments may appear (e.g., `практика.ipynb`).
