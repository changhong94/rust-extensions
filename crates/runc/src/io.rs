/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
#[cfg(not(feature = "async"))]
use std::io::{Read, Write};
use std::{
    fmt::Debug,
    fs::{File, OpenOptions},
    io::Result,
    os::unix::{
        fs::OpenOptionsExt,
        io::{AsRawFd, OwnedFd},
    },
    process::Stdio,
    sync::Mutex,
};

use nix::unistd::{Gid, Uid};
#[cfg(feature = "async")]
use tokio::io::{AsyncRead, AsyncWrite};

use crate::Command;

pub trait Io: Debug + Send + Sync {
    /// Return write side of stdin
    #[cfg(not(feature = "async"))]
    fn stdin(&self) -> Option<Box<dyn Write + Send + Sync>> {
        None
    }

    /// Return read side of stdout
    #[cfg(not(feature = "async"))]
    fn stdout(&self) -> Option<Box<dyn Read + Send>> {
        None
    }

    /// Return read side of stderr
    #[cfg(not(feature = "async"))]
    fn stderr(&self) -> Option<Box<dyn Read + Send>> {
        None
    }

    /// Return write side of stdin
    #[cfg(feature = "async")]
    fn stdin(&self) -> Option<Box<dyn AsyncWrite + Send + Sync + Unpin>> {
        None
    }

    /// Return read side of stdout
    #[cfg(feature = "async")]
    fn stdout(&self) -> Option<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        None
    }

    /// Return read side of stderr
    #[cfg(feature = "async")]
    fn stderr(&self) -> Option<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        None
    }

    /// Set IO for passed command.
    /// Read side of stdin, write side of stdout and write side of stderr should be provided to command.
    fn set(&self, cmd: &mut Command) -> Result<()>;

    /// Only close write side (should be stdout/err "from" runc process)
    fn close_after_start(&self);
}

#[derive(Debug, Clone)]
pub struct IOOption {
    pub open_stdin: bool,
    pub open_stdout: bool,
    pub open_stderr: bool,
}

impl Default for IOOption {
    fn default() -> Self {
        Self {
            open_stdin: true,
            open_stdout: true,
            open_stderr: true,
        }
    }
}

/// Struct to represent a pipe that can be used to transfer stdio inputs and outputs.
///
/// With this Io driver, methods of [crate::Runc] may capture the output/error messages.
/// When one side of the pipe is closed, the state will be represented with [`None`].
#[derive(Debug)]
pub struct Pipe {
    rd: Mutex<Option<File>>,
    wr: Mutex<Option<File>>,
}

#[derive(Debug)]
pub struct PipedIo {
    stdin: Option<Pipe>,
    stdout: Option<Pipe>,
    stderr: Option<Pipe>,
}

impl Pipe {
    fn new() -> std::io::Result<Self> {
        let (rd, wr) = os_pipe::pipe()?;
        Ok(Self {
            rd: Mutex::new(Some(File::from(OwnedFd::from(rd)))),
            wr: Mutex::new(Some(File::from(OwnedFd::from(wr)))),
        })
    }
}

impl PipedIo {
    pub fn new(uid: u32, gid: u32, opts: &IOOption) -> std::io::Result<Self> {
        Ok(Self {
            stdin: Self::create_pipe(uid, gid, opts.open_stdin, true)?,
            stdout: Self::create_pipe(uid, gid, opts.open_stdout, false)?,
            stderr: Self::create_pipe(uid, gid, opts.open_stderr, false)?,
        })
    }

    fn create_pipe(
        uid: u32,
        gid: u32,
        enabled: bool,
        stdin: bool,
    ) -> std::io::Result<Option<Pipe>> {
        if !enabled {
            return Ok(None);
        }
        let pipe = Pipe::new()?;
        let end = if stdin { &pipe.rd } else { &pipe.wr };
        nix::unistd::fchown(
            end.lock().unwrap().as_ref().unwrap().as_raw_fd(),
            Some(Uid::from_raw(uid)),
            Some(Gid::from_raw(gid)),
        )?;
        Ok(Some(pipe))
    }
}

impl Io for PipedIo {
    #[cfg(not(feature = "async"))]
    fn stdin(&self) -> Option<Box<dyn Write + Send + Sync>> {
        Some(Box::new(self.stdin.as_ref()?.wr.lock().unwrap().take()?))
    }
    #[cfg(feature = "async")]
    fn stdin(&self) -> Option<Box<dyn AsyncWrite + Send + Sync + Unpin>> {
        let fd = self.stdin.as_ref()?.wr.lock().unwrap().take()?;
        tokio::net::unix::pipe::Sender::from_owned_fd(fd.into())
            .ok()
            .map(|p| Box::new(p) as Box<dyn AsyncWrite + Send + Sync + Unpin>)
    }
    #[cfg(not(feature = "async"))]
    fn stdout(&self) -> Option<Box<dyn Read + Send>> {
        Some(Box::new(self.stdout.as_ref()?.rd.lock().unwrap().take()?))
    }
    #[cfg(feature = "async")]
    fn stdout(&self) -> Option<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        let fd = self.stdout.as_ref()?.rd.lock().unwrap().take()?;
        tokio::net::unix::pipe::Receiver::from_owned_fd(fd.into())
            .ok()
            .map(|p| Box::new(p) as Box<dyn AsyncRead + Send + Sync + Unpin>)
    }
    #[cfg(not(feature = "async"))]
    fn stderr(&self) -> Option<Box<dyn Read + Send>> {
        Some(Box::new(self.stderr.as_ref()?.rd.lock().unwrap().take()?))
    }
    #[cfg(feature = "async")]
    fn stderr(&self) -> Option<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        let fd = self.stderr.as_ref()?.rd.lock().unwrap().take()?;
        tokio::net::unix::pipe::Receiver::from_owned_fd(fd.into())
            .ok()
            .map(|p| Box::new(p) as Box<dyn AsyncRead + Send + Sync + Unpin>)
    }

    fn set(&self, cmd: &mut Command) -> Result<()> {
        // Command owns clones, never another owner of the same descriptor.
        // Originals remain owned here until after_start or cancellation/drop.
        if let Some(p) = self.stdin.as_ref() {
            if let Some(file) = p.rd.lock().unwrap().as_ref() {
                cmd.stdin(Stdio::from(file.try_clone()?));
            }
        }
        if let Some(p) = self.stdout.as_ref() {
            if let Some(file) = p.wr.lock().unwrap().as_ref() {
                cmd.stdout(Stdio::from(file.try_clone()?));
            }
        }
        if let Some(p) = self.stderr.as_ref() {
            if let Some(file) = p.wr.lock().unwrap().as_ref() {
                cmd.stderr(Stdio::from(file.try_clone()?));
            }
        }
        Ok(())
    }

    fn close_after_start(&self) {
        // Idempotently release only the child ends still owned by this object.
        if let Some(p) = self.stdin.as_ref() {
            p.rd.lock().unwrap().take();
        }
        if let Some(p) = self.stdout.as_ref() {
            p.wr.lock().unwrap().take();
        }
        if let Some(p) = self.stderr.as_ref() {
            p.wr.lock().unwrap().take();
        }
    }
}

/// IO driver to direct output/error messages to /dev/null.
///
/// With this Io driver, all methods of [crate::Runc] can't capture the output/error messages.
#[derive(Debug)]
pub struct NullIo {
    dev_null: Mutex<Option<File>>,
}

impl NullIo {
    pub fn new() -> std::io::Result<Self> {
        let f = OpenOptions::new().read(true).open("/dev/null")?;
        let dev_null = Mutex::new(Some(f));
        Ok(Self { dev_null })
    }
}

impl Io for NullIo {
    fn set(&self, cmd: &mut Command) -> std::io::Result<()> {
        if let Some(null) = self.dev_null.lock().unwrap().as_ref() {
            cmd.stdout(null.try_clone()?);
            cmd.stderr(null.try_clone()?);
        }
        Ok(())
    }

    fn close_after_start(&self) {
        let mut m = self.dev_null.lock().unwrap();
        let _ = m.take();
    }
}

/// Io driver based on Stdio::inherited(), to direct outputs/errors to stdio.
///
/// With this Io driver, all methods of [crate::Runc] can't capture the output/error messages.
#[derive(Debug)]
pub struct InheritedStdIo {}

impl InheritedStdIo {
    pub fn new() -> std::io::Result<Self> {
        Ok(InheritedStdIo {})
    }
}

impl Io for InheritedStdIo {
    fn set(&self, cmd: &mut Command) -> std::io::Result<()> {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        Ok(())
    }

    fn close_after_start(&self) {}
}

/// Io driver based on Stdio::piped(), to capture outputs/errors from runC.
///
/// With this Io driver, methods of [crate::Runc] may capture the output/error messages.
#[derive(Debug)]
pub struct PipedStdIo {}

impl PipedStdIo {
    pub fn new() -> std::io::Result<Self> {
        Ok(PipedStdIo {})
    }
}

impl Io for PipedStdIo {
    fn set(&self, cmd: &mut Command) -> std::io::Result<()> {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok(())
    }

    fn close_after_start(&self) {}
}

/// FIFO for the scenario that set FIFO for command Io.
#[derive(Debug)]
pub struct FIFO {
    pub stdin: Option<String>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
}

impl Io for FIFO {
    fn set(&self, cmd: &mut Command) -> Result<()> {
        if let Some(path) = self.stdin.as_ref() {
            let stdin = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(path)?;
            cmd.stdin(stdin);
        }

        if let Some(path) = self.stdout.as_ref() {
            let stdout = OpenOptions::new().write(true).open(path)?;
            cmd.stdout(stdout);
        }

        if let Some(path) = self.stderr.as_ref() {
            let stderr = OpenOptions::new().write(true).open(path)?;
            cmd.stderr(stderr);
        }

        Ok(())
    }

    fn close_after_start(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn pipe_endpoints_are_claimed_once_and_close_with_their_owners() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let io = PipedIo::new(
            nix::unistd::getuid().as_raw(),
            nix::unistd::getgid().as_raw(),
            &IOOption::default(),
        )
        .unwrap();
        let mut input = io.stdin().unwrap();
        let mut output = io.stdout().unwrap();
        let mut error = io.stderr().unwrap();
        assert!(io.stdin().is_none() && io.stdout().is_none() && io.stderr().is_none());
        let mut command = Command::new("/bin/cat");
        io.set(&mut command).unwrap();
        let mut child = command.spawn().unwrap();
        drop(command);
        io.close_after_start();
        io.close_after_start();
        drop(io);
        input.write_all(b"owned pipe bytes").await.unwrap();
        drop(input);
        let mut bytes = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            output.read_to_end(&mut bytes),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(bytes, b"owned pipe bytes");
        assert_eq!(error.read(&mut [0]).await.unwrap(), 0);
        assert!(child.wait().await.unwrap().success());
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn unused_child_pipe_end_is_closed_on_cancel() {
        use tokio::io::AsyncReadExt;
        let io = PipedIo::new(
            nix::unistd::getuid().as_raw(),
            nix::unistd::getgid().as_raw(),
            &IOOption {
                open_stdin: false,
                open_stdout: true,
                open_stderr: false,
            },
        )
        .unwrap();
        let mut output = io.stdout().unwrap();
        drop(io);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), output.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }

    #[test]
    fn command_drop_must_not_leave_a_stale_fd_for_after_start() {
        let io = PipedIo::new(
            nix::unistd::getuid().as_raw(),
            nix::unistd::getgid().as_raw(),
            &IOOption {
                open_stdin: false,
                open_stdout: true,
                open_stderr: false,
            },
        )
        .unwrap();
        let original = io
            .stdout
            .as_ref()
            .unwrap()
            .wr
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_raw_fd();
        let mut command = Command::new("/bin/true");
        io.set(&mut command).unwrap();
        drop(command);
        let file = File::open("/dev/null").unwrap();
        use std::os::unix::io::AsRawFd;
        // Reserve an available descriptor at or above the former pipe endpoint.
        // It has no automatic owner, so the broken implementation cannot cause
        // a second destructor close after the assertion detects its mistake.
        let probe = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, original) };
        assert!(probe >= 0);
        io.close_after_start();
        let valid = unsafe { libc::fcntl(probe, libc::F_GETFD) } >= 0;
        if valid {
            unsafe {
                libc::close(probe);
            }
        }
        assert!(valid, "after_start closed an unrelated reused FD");
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn test_io_option() {
        let opts = IOOption {
            open_stdin: false,
            open_stdout: false,
            open_stderr: false,
        };
        let io = PipedIo::new(1000, 1000, &opts).unwrap();

        assert!(io.stdin().is_none());
        assert!(io.stdout().is_none());
        assert!(io.stderr().is_none());
    }

    #[cfg(target_os = "linux")]
    #[cfg(not(feature = "async"))]
    #[test]
    fn test_create_piped_io() {
        let opts = IOOption::default();
        let uid = nix::unistd::getuid();
        let gid = nix::unistd::getgid();
        let io = PipedIo::new(uid.as_raw(), gid.as_raw(), &opts).unwrap();
        let mut buf = [0xfau8];

        let mut stdin = io.stdin().unwrap();
        stdin.write_all(&buf).unwrap();
        buf[0] = 0x0;

        io.stdin.as_ref().map(|v| {
            v.rd.lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .try_clone()
                .unwrap()
                .read(&mut buf)
                .unwrap()
        });
        assert_eq!(&buf, &[0xfau8]);

        let mut stdout = io.stdout().unwrap();
        buf[0] = 0xce;
        io.stdout.as_ref().map(|v| {
            v.wr.lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .try_clone()
                .unwrap()
                .write(&buf)
                .unwrap()
        });
        buf[0] = 0x0;
        stdout.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[0xceu8]);

        let mut stderr = io.stderr().unwrap();
        buf[0] = 0xa5;
        io.stderr.as_ref().map(|v| {
            v.wr.lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .try_clone()
                .unwrap()
                .write(&buf)
                .unwrap()
        });
        buf[0] = 0x0;
        stderr.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &[0xa5u8]);

        io.close_after_start();
        stdout.read_exact(&mut buf).unwrap_err();
        stderr.read_exact(&mut buf).unwrap_err();
    }

    #[test]
    fn test_null_io() {
        let io = NullIo::new().unwrap();
        assert!(io.stdin().is_none());
        assert!(io.stdout().is_none());
        assert!(io.stderr().is_none());
    }
}
