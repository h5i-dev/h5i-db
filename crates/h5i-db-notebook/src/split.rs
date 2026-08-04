//! Opening a command in a pane beside the caller.
//!
//! This is what lets an agent put a notebook next to the conversation instead
//! of describing it: `nb watch <file> --split right` spawns the pane and
//! returns immediately, so the call costs a turn rather than blocking on a UI
//! a human will sit in for an hour.
//!
//! Every multiplexer here can already do the work; the only thing worth
//! writing is the translation, and the rule that a terminal we do not
//! recognise is an error rather than a fallback. Falling back to "open in the
//! current pane" would take over the terminal the agent is talking in, which
//! is the single surprise this feature exists to prevent.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Right,
    Left,
    Down,
    Up,
}

impl Direction {
    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "right" | "r" => Ok(Direction::Right),
            "left" | "l" => Ok(Direction::Left),
            "down" | "d" | "below" => Ok(Direction::Down),
            "up" | "u" | "above" => Ok(Direction::Up),
            other => Err(Error::invalid(format!(
                "unknown split direction {other:?}; supported: right, left, down, up"
            ))),
        }
    }
}

/// A terminal multiplexer that can be asked for a new pane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Multiplexer {
    Tmux,
    Kitty,
    WezTerm,
    Zellij,
}

impl Multiplexer {
    /// The variable whose presence identifies this multiplexer.
    fn marker(self) -> &'static str {
        match self {
            Multiplexer::Tmux => "TMUX",
            Multiplexer::Kitty => "KITTY_WINDOW_ID",
            Multiplexer::WezTerm => "WEZTERM_PANE",
            Multiplexer::Zellij => "ZELLIJ",
        }
    }
}

/// Multiplexers in the order they are tested.
///
/// tmux first, deliberately: inside tmux running in kitty both markers are
/// set, and the pane the human is looking at is tmux's.
const CANDIDATES: [Multiplexer; 4] = [
    Multiplexer::Tmux,
    Multiplexer::Zellij,
    Multiplexer::WezTerm,
    Multiplexer::Kitty,
];

/// Which multiplexer `env` describes, if any.
///
/// Takes the lookup rather than reading the environment so the decision is
/// testable without touching the process's own.
pub fn detect(env: impl Fn(&str) -> Option<OsString>) -> Option<Multiplexer> {
    CANDIDATES
        .into_iter()
        .find(|m| env(m.marker()).is_some_and(|v| !v.is_empty()))
}

pub fn detect_from_env() -> Option<Multiplexer> {
    detect(|name: &str| std::env::var_os(name))
}

/// The program and arguments that ask `mux` to run `argv` in a new pane.
pub fn pane_command(
    mux: Multiplexer,
    direction: Direction,
    argv: &[String],
) -> (String, Vec<String>) {
    match mux {
        // The only one that takes a shell string rather than an argv, so the
        // only one that needs quoting.
        Multiplexer::Tmux => {
            let mut args = vec!["split-window".to_string()];
            args.extend(match direction {
                Direction::Right => vec!["-h".to_string()],
                Direction::Left => vec!["-h".to_string(), "-b".to_string()],
                Direction::Down => vec!["-v".to_string()],
                Direction::Up => vec!["-v".to_string(), "-b".to_string()],
            });
            args.push(shell_join(argv));
            ("tmux".to_string(), args)
        }
        Multiplexer::Zellij => {
            let where_to = match direction {
                Direction::Right => "right",
                Direction::Left => "left",
                Direction::Down => "down",
                Direction::Up => "up",
            };
            let mut args = vec![
                "action".to_string(),
                "new-pane".to_string(),
                "-d".to_string(),
                where_to.to_string(),
                "--".to_string(),
            ];
            args.extend(argv.iter().cloned());
            ("zellij".to_string(), args)
        }
        Multiplexer::WezTerm => {
            let where_to = match direction {
                Direction::Right => "--right",
                Direction::Left => "--left",
                Direction::Down => "--bottom",
                Direction::Up => "--top",
            };
            let mut args = vec![
                "cli".to_string(),
                "split-pane".to_string(),
                where_to.to_string(),
                "--".to_string(),
            ];
            args.extend(argv.iter().cloned());
            ("wezterm".to_string(), args)
        }
        Multiplexer::Kitty => {
            // kitty splits along an axis and places the new window after the
            // current one; it has no "before" the way tmux's `-b` does, so
            // left and up land on the same side as right and down. Better a
            // pane on the wrong side than a refused command.
            let location = match direction {
                Direction::Right | Direction::Left => "--location=vsplit",
                Direction::Down | Direction::Up => "--location=hsplit",
            };
            let mut args = vec![
                "@".to_string(),
                "launch".to_string(),
                "--type=window".to_string(),
                location.to_string(),
                "--cwd=current".to_string(),
            ];
            args.extend(argv.iter().cloned());
            ("kitten".to_string(), args)
        }
    }
}

/// Run `argv` in a new pane beside this one.
///
/// Returns once the pane has been created, not when the command in it exits:
/// the whole point is that the caller carries on.
pub fn spawn(direction: Direction, argv: &[String]) -> Result<Multiplexer> {
    let Some(mux) = detect_from_env() else {
        return Err(Error::invalid(
            "no terminal multiplexer to split: none of $TMUX, $ZELLIJ, \
             $WEZTERM_PANE or $KITTY_WINDOW_ID is set. Run this inside tmux, \
             zellij, WezTerm or kitty, or drop --split and run it here",
        ));
    };
    let (program, args) = pane_command(mux, direction, argv);

    let status = std::process::Command::new(&program)
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|e| {
            Error::invalid(format!(
                "cannot run `{program}` to open a pane ({e}); it has to be on PATH for --split"
            ))
        })?;

    if !status.success() {
        return Err(Error::invalid(match mux {
            // Far and away the most common cause, and unguessable: kitty ships
            // with remote control off.
            Multiplexer::Kitty => format!(
                "`{program}` refused to open a pane. kitty needs \
                 `allow_remote_control yes` in kitty.conf for --split to work"
            ),
            _ => format!("`{program}` refused to open a pane (exit {status})"),
        }));
    }
    Ok(mux)
}

/// This binary, plus the subcommand path that reaches the notebook commands.
///
/// The pane re-executes us rather than looking for `h5i-nb` on `PATH`, for the
/// same reason the supervisor does: the command tree is mounted differently in
/// each binary, and whichever one the human is already running is the one that
/// works.
pub fn self_command(arguments: &[String]) -> Result<Vec<String>> {
    let exe: PathBuf = std::env::current_exe()
        .map_err(|e| Error::internal(format!("cannot locate our own binary: {e}")))?;
    let mut argv = vec![exe.display().to_string()];
    argv.extend(crate::supervisor::client::command_prefix().iter().cloned());
    argv.extend(arguments.iter().cloned());
    Ok(argv)
}

/// One shell word, quoted if it needs to be.
///
/// Single quotes, because inside them a POSIX shell interprets nothing at all;
/// the only character that needs care is the single quote itself.
fn shell_quote(word: &str) -> String {
    if !word.is_empty()
        && word
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./=:,@+".contains(c))
    {
        return word.to_string();
    }
    format!("'{}'", word.replace('\'', r"'\''"))
}

fn shell_join(argv: &[String]) -> String {
    argv.iter()
        .map(|word| shell_quote(word))
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> + use<> {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| {
            owned
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| OsString::from(v))
        }
    }

    #[test]
    fn each_multiplexer_is_recognised_by_its_own_marker() {
        assert_eq!(
            detect(env_of(&[("TMUX", "/tmp/tmux-1000/default,123,0")])),
            Some(Multiplexer::Tmux)
        );
        assert_eq!(
            detect(env_of(&[("ZELLIJ", "0")])),
            Some(Multiplexer::Zellij)
        );
        assert_eq!(
            detect(env_of(&[("WEZTERM_PANE", "3")])),
            Some(Multiplexer::WezTerm)
        );
        assert_eq!(
            detect(env_of(&[("KITTY_WINDOW_ID", "1")])),
            Some(Multiplexer::Kitty)
        );
        assert_eq!(detect(env_of(&[("TERM", "xterm-256color")])), None);
    }

    #[test]
    fn an_empty_marker_does_not_count() {
        // Exported-but-empty is how a variable survives an ssh or a `env -i`
        // that meant to clear it.
        assert_eq!(detect(env_of(&[("TMUX", "")])), None);
    }

    #[test]
    fn tmux_wins_when_it_is_running_inside_another_terminal() {
        // Both markers are set inside tmux-in-kitty, and the pane the human is
        // looking at belongs to tmux.
        let env = env_of(&[("KITTY_WINDOW_ID", "1"), ("TMUX", "/tmp/s,1,0")]);
        assert_eq!(detect(env), Some(Multiplexer::Tmux));
    }

    #[test]
    fn tmux_gets_one_shell_string_with_awkward_arguments_quoted() {
        let argv = vec![
            "/opt/my tools/h5i-nb".to_string(),
            "watch".to_string(),
            "/tmp/it's here/nb.ipynb".to_string(),
        ];
        let (program, args) = pane_command(Multiplexer::Tmux, Direction::Right, &argv);
        assert_eq!(program, "tmux");
        assert_eq!(args[0], "split-window");
        assert_eq!(args[1], "-h");
        // One argument, not three, and safe to hand to a shell.
        assert_eq!(args.len(), 3);
        assert_eq!(
            args[2],
            r"'/opt/my tools/h5i-nb' watch '/tmp/it'\''s here/nb.ipynb'"
        );
    }

    #[test]
    fn tmux_directions_map_to_the_axis_and_the_before_flag() {
        let argv = vec!["x".to_string()];
        let flags = |d| {
            let (_, args) = pane_command(Multiplexer::Tmux, d, &argv);
            args[1..args.len() - 1].to_vec()
        };
        assert_eq!(flags(Direction::Right), ["-h"]);
        assert_eq!(flags(Direction::Left), ["-h", "-b"]);
        assert_eq!(flags(Direction::Down), ["-v"]);
        assert_eq!(flags(Direction::Up), ["-v", "-b"]);
    }

    #[test]
    fn the_argv_multiplexers_get_the_arguments_verbatim() {
        // No quoting anywhere: these take an argv, so a path with a space in
        // it must arrive as one word rather than as shell syntax.
        let argv = vec![
            "/opt/my tools/h5i-nb".to_string(),
            "watch".to_string(),
            "/tmp/a b.ipynb".to_string(),
        ];
        for mux in [
            Multiplexer::Zellij,
            Multiplexer::WezTerm,
            Multiplexer::Kitty,
        ] {
            let (_, args) = pane_command(mux, Direction::Right, &argv);
            let tail = &args[args.len() - 3..];
            assert_eq!(tail, argv.as_slice(), "{mux:?} mangled the arguments");
        }
    }

    #[test]
    fn wezterm_and_zellij_name_the_side_they_are_given() {
        let argv = vec!["x".to_string()];
        let (_, args) = pane_command(Multiplexer::WezTerm, Direction::Down, &argv);
        assert!(args.contains(&"--bottom".to_string()), "{args:?}");
        let (_, args) = pane_command(Multiplexer::Zellij, Direction::Up, &argv);
        assert!(args.contains(&"up".to_string()), "{args:?}");
    }

    #[test]
    fn directions_parse_from_what_a_person_would_type() {
        assert_eq!(Direction::parse("right").unwrap(), Direction::Right);
        assert_eq!(Direction::parse("DOWN").unwrap(), Direction::Down);
        assert_eq!(Direction::parse("below").unwrap(), Direction::Down);
        let error = Direction::parse("sideways").unwrap_err();
        assert_eq!(error.code(), "invalid_input");
        assert!(error.to_string().contains("right"), "{error}");
    }

    #[test]
    fn quoting_leaves_ordinary_words_alone() {
        assert_eq!(shell_quote("watch"), "watch");
        assert_eq!(shell_quote("/tmp/nb.ipynb"), "/tmp/nb.ipynb");
        assert_eq!(shell_quote("--split=right"), "--split=right");
        assert_eq!(shell_quote(""), "''");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("$(rm -rf /)"), "'$(rm -rf /)'");
    }
}
