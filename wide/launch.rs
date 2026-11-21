use std::{env, io, os::unix::process::CommandExt, process::Command};

fn use_wide() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        env::var_os("SHOPPING_SCANNER").as_deref() != Some(std::ffi::OsStr::new("avx2"))
            && is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx512bw")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

fn main() -> io::Result<()> {
    let executable = env::current_exe()?.with_file_name(if use_wide() {
        "shopping-sessions-wide"
    } else {
        "shopping-sessions-stock"
    });
    Err(Command::new(executable).args(env::args_os().skip(1)).exec())
}
