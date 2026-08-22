//! Harness metrics. The actionable axis is binary: machine (positive) vs live
//! human. Precision/recall/F1 on that axis gate a Phase-2 hangup.

use crate::amd::AmdClass;

/// Precision/recall/F1 with "machine" as the positive class.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BinaryMetrics {
    pub precision: f32,
    pub recall: f32,
    pub f1: f32,
}

/// `pairs` are `(predicted, actual)`.
pub fn binary_metrics(pairs: &[(AmdClass, AmdClass)]) -> BinaryMetrics {
    let (mut tp, mut fp, mut fn_) = (0u32, 0u32, 0u32);
    for (pred, actual) in pairs {
        match (pred.is_machine(), actual.is_machine()) {
            (true, true) => tp += 1,
            (true, false) => fp += 1,
            (false, true) => fn_ += 1,
            (false, false) => {}
        }
    }
    let precision = if tp + fp == 0 {
        0.0
    } else {
        tp as f32 / (tp + fp) as f32
    };
    let recall = if tp + fn_ == 0 {
        0.0
    } else {
        tp as f32 / (tp + fn_) as f32
    };
    let f1 = if precision + recall == 0.0 {
        0.0
    } else {
        2.0 * precision * recall / (precision + recall)
    };
    BinaryMetrics {
        precision,
        recall,
        f1,
    }
}

/// Number of classes — the confusion matrix is `N`×`N`.
pub const N: usize = AmdClass::ALL.len();

/// Confusion counts indexed `[actual][predicted]` over [`AmdClass::ALL`].
pub fn confusion(pairs: &[(AmdClass, AmdClass)]) -> [[u32; N]; N] {
    let mut m = [[0u32; N]; N];
    for (pred, actual) in pairs {
        m[actual.index()][pred.index()] += 1;
    }
    m
}

/// Render a confusion matrix (rows = actual, columns = predicted) as text.
pub fn render_confusion(m: &[[u32; N]; N]) -> String {
    let mut s = String::from("confusion (rows=actual, cols=predicted):\n");
    s.push_str(&format!("{:>10}", ""));
    for c in AmdClass::ALL {
        s.push_str(&format!("{:>10}", c.label()));
    }
    s.push('\n');
    for actual in AmdClass::ALL {
        s.push_str(&format!("{:>10}", actual.label()));
        for pred in AmdClass::ALL {
            s.push_str(&format!("{:>10}", m[actual.index()][pred.index()]));
        }
        s.push('\n');
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amd::AmdClass::*;

    #[test]
    fn binary_metrics_on_machine_positive() {
        // predicted, actual. Positive = machine.
        // TP: predict machine & is machine; FP: predict machine & is human;
        // FN: predict human & is machine.
        let pairs = vec![
            (Voicemail, Voicemail), // TP
            (Hold, Ivr),            // TP (both machine)
            (Voicemail, Human),     // FP
            (Human, Voicemail),     // FN
            (Human, Human),         // TN
        ];
        let m = binary_metrics(&pairs);
        // TP=2, FP=1, FN=1 -> precision 2/3, recall 2/3, f1 2/3.
        assert!((m.precision - 2.0 / 3.0).abs() < 1e-6);
        assert!((m.recall - 2.0 / 3.0).abs() < 1e-6);
        assert!((m.f1 - 2.0 / 3.0).abs() < 1e-6);
    }

    #[test]
    fn no_positives_predicted_is_zero_precision() {
        let pairs = vec![(Human, Voicemail), (Human, Human)];
        let m = binary_metrics(&pairs);
        assert_eq!(m.precision, 0.0);
        assert_eq!(m.recall, 0.0);
    }

    #[test]
    fn confusion_counts_by_actual_then_predicted() {
        let pairs = vec![
            (Human, Human),         // actual human, predicted human
            (Voicemail, Human),     // actual human, predicted voicemail
            (Voicemail, Voicemail), // actual voicemail, predicted voicemail
            (Hold, Ivr),            // actual ivr, predicted hold
        ];
        let m = confusion(&pairs);
        assert_eq!(m[Human.index()][Human.index()], 1);
        assert_eq!(m[Human.index()][Voicemail.index()], 1);
        assert_eq!(m[Voicemail.index()][Voicemail.index()], 1);
        assert_eq!(m[Ivr.index()][Hold.index()], 1);
        // A cell with no observations stays zero.
        assert_eq!(m[Hold.index()][Hold.index()], 0);
    }
}
