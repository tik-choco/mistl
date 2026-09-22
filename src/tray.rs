//! Windows notification-area UI. The daemon owns all state; the tray is a client.
use crate::daemon::AppState;
use std::sync::Arc;

#[cfg(not(windows))]
pub fn spawn(_state: Arc<AppState>) {}

#[cfg(windows)]
pub use windows::spawn;

#[cfg(windows)]
mod windows {
    use super::*;
    use std::{
        cell::RefCell,
        ptr::{null, null_mut},
        sync::atomic::{AtomicUsize, Ordering},
    };
    use windows_sys::Win32::{
        Foundation::*,
        System::LibraryLoader::GetModuleHandleW,
        UI::{Shell::*, WindowsAndMessaging::*},
    };

    const CALLBACK: u32 = WM_APP + 1;
    const TOGGLE: usize = 1;
    const OPEN: usize = 2;
    const AUTOSTART: usize = 3;
    const EXIT: usize = 4;
    struct Context {
        state: Arc<AppState>,
        runtime: tokio::runtime::Handle,
        taskbar_created: u32,
    }
    thread_local! { static CONTEXT: RefCell<Option<Context>> = const { RefCell::new(None) }; }
    pub struct TrayGuard {
        window: Arc<AtomicUsize>,
    }
    impl Drop for TrayGuard {
        fn drop(&mut self) {
            let window = self.window.load(Ordering::Acquire) as HWND;
            if !window.is_null() {
                // SAFETY: message is posted to a window owned by the tray thread.
                unsafe {
                    PostMessageW(window, WM_CLOSE, 0, 0);
                }
            }
        }
    }
    fn wide(text: &str) -> Vec<u16> {
        text.encode_utf16().chain(Some(0)).collect()
    }

    pub fn spawn(state: Arc<AppState>) -> Option<TrayGuard> {
        if crate::runtime::context().no_tray {
            return None;
        }
        let window = Arc::new(AtomicUsize::new(0));
        let slot = window.clone();
        let runtime = tokio::runtime::Handle::current();
        let result = std::thread::Builder::new()
            .name("mistl-tray".into())
            .spawn(move || {
                // SAFETY: all HWND/HMENU/notification operations remain on this thread;
                // UTF-16 buffers and window class remain alive throughout their calls.
                unsafe {
                    let class = wide("MISTL.NotificationArea");
                    let instance = GetModuleHandleW(null());
                    let wc = WNDCLASSW {
                        lpfnWndProc: Some(window_proc),
                        hInstance: instance,
                        lpszClassName: class.as_ptr(),
                        ..std::mem::zeroed()
                    };
                    if RegisterClassW(&wc) == 0 {
                        tracing::warn!("could not register tray window class");
                        return;
                    }
                    let taskbar_created = RegisterWindowMessageW(wide("TaskbarCreated").as_ptr());
                    CONTEXT.with(|c| {
                        *c.borrow_mut() = Some(Context {
                            state,
                            runtime,
                            taskbar_created,
                        })
                    });
                    let hwnd = CreateWindowExW(
                        0,
                        class.as_ptr(),
                        class.as_ptr(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        null_mut(),
                        null_mut(),
                        instance,
                        null(),
                    );
                    if hwnd.is_null() {
                        tracing::warn!("could not create tray window");
                        return;
                    }
                    slot.store(hwnd as usize, Ordering::Release);
                    notify(hwnd, NIM_ADD);
                    SetTimer(hwnd, 1, 1000, None);
                    let mut message: MSG = std::mem::zeroed();
                    while GetMessageW(&mut message, null_mut(), 0, 0) > 0 {
                        TranslateMessage(&message);
                        DispatchMessageW(&message);
                    }
                    slot.store(0, Ordering::Release);
                    CONTEXT.with(|c| *c.borrow_mut() = None);
                }
            });
        if let Err(error) = result {
            tracing::warn!(%error, "could not start tray thread");
            return None;
        }
        Some(TrayGuard { window })
    }

    unsafe fn notify(hwnd: HWND, action: u32) {
        CONTEXT.with(|cell| {
            let borrow = cell.borrow();
            let Some(ctx) = borrow.as_ref() else {
                return;
            };
            let network = ctx.state.network.status();
            let mode = network["state"].as_str().unwrap_or("error");
            let title = format!(
                "MISTL {} / {} / v{} / {}",
                crate::runtime::CHANNEL,
                crate::runtime::context().instance,
                env!("CARGO_PKG_VERSION"),
                mode.to_uppercase()
            );
            // SAFETY: struct has the native layout provided by windows-sys.
            unsafe {
                let mut icon: NOTIFYICONDATAW = std::mem::zeroed();
                icon.cbSize = std::mem::size_of::<NOTIFYICONDATAW>() as u32;
                icon.hWnd = hwnd;
                icon.uID = 1;
                icon.uFlags = NIF_ICON | NIF_MESSAGE | NIF_TIP;
                icon.uCallbackMessage = CALLBACK;
                icon.hIcon = LoadIconW(
                    null_mut(),
                    if mode == "on" {
                        IDI_INFORMATION
                    } else {
                        IDI_WARNING
                    },
                );
                for (dest, value) in icon.szTip.iter_mut().take(127).zip(title.encode_utf16()) {
                    *dest = value;
                }
                Shell_NotifyIconW(action, &icon);
            }
        });
    }

    unsafe fn menu(hwnd: HWND) {
        // Copy state before TrackPopupMenu enters its nested Windows message loop.
        let data = CONTEXT.with(|c| {
            c.borrow()
                .as_ref()
                .map(|c| (c.state.network.status(), c.state.clone(), c.runtime.clone()))
        });
        let Some((network, state, runtime)) = data else {
            return;
        };
        // SAFETY: menu is created, used, and destroyed on this window's thread.
        unsafe {
            let menu = CreatePopupMenu();
            if menu.is_null() {
                return;
            }
            let heading = format!(
                "MISTL {} · {} · v{}",
                crate::runtime::CHANNEL.to_uppercase(),
                crate::runtime::context().instance,
                env!("CARGO_PKG_VERSION")
            );
            AppendMenuW(menu, MF_STRING | MF_GRAYED, 0, wide(&heading).as_ptr());
            let current = network["state"].as_str().unwrap_or("error");
            let label = format!(
                "外部接続: {}{}",
                current.to_uppercase(),
                if network["saved"] == true {
                    "（保存済み）"
                } else {
                    "（未保存・エラー）"
                }
            );
            AppendMenuW(menu, MF_STRING | MF_GRAYED, 0, wide(&label).as_ptr());
            AppendMenuW(menu, MF_SEPARATOR, 0, null());
            let enabled = network["enabled"] == true;
            AppendMenuW(
                menu,
                MF_STRING
                    | if current == "restarting" {
                        MF_GRAYED
                    } else {
                        0
                    },
                TOGGLE,
                wide(if enabled {
                    "外部接続を OFF にする"
                } else {
                    "外部接続を ON にする"
                })
                .as_ptr(),
            );
            AppendMenuW(menu, MF_STRING, OPEN, wide("ダッシュボードを開く").as_ptr());
            let flags = MF_STRING
                | if crate::install::autostart_enabled() {
                    MF_CHECKED
                } else {
                    0
                };
            AppendMenuW(menu, flags, AUTOSTART, wide("ログイン時に起動").as_ptr());
            AppendMenuW(menu, MF_SEPARATOR, 0, null());
            AppendMenuW(
                menu,
                MF_STRING,
                EXIT,
                wide("このインスタンスを終了").as_ptr(),
            );
            let mut point: POINT = std::mem::zeroed();
            GetCursorPos(&mut point);
            SetForegroundWindow(hwnd);
            let selected = TrackPopupMenu(
                menu,
                TPM_RETURNCMD | TPM_RIGHTBUTTON,
                point.x,
                point.y,
                0,
                hwnd,
                null(),
            ) as usize;
            PostMessageW(hwnd, WM_NULL, 0, 0);
            DestroyMenu(menu);
            match selected {
                TOGGLE => {
                    runtime.spawn(async move {
                        if let Err(error) = crate::daemon::dispatch(
                            "network.set",
                            serde_json::json!({"enabled":!enabled}),
                            &state,
                        )
                        .await
                        {
                            tracing::error!(%error, "tray connection toggle failed");
                        }
                    });
                }
                OPEN => {
                    if let Some(url) = state.dashboard_url() {
                        crate::web::browser::open_in_browser(&url);
                    }
                }
                AUTOSTART => {
                    if let Err(error) =
                        crate::install::set_autostart(!crate::install::autostart_enabled())
                    {
                        MessageBoxW(
                            hwnd,
                            wide(&format!("{error:#}")).as_ptr(),
                            wide("MISTL").as_ptr(),
                            MB_OK | MB_ICONERROR,
                        );
                    }
                }
                EXIT => state.request_shutdown(),
                _ => {}
            }
        }
    }
    unsafe extern "system" fn window_proc(
        hwnd: HWND,
        message: u32,
        wparam: WPARAM,
        lparam: LPARAM,
    ) -> LRESULT {
        // SAFETY: Windows invokes this callback with a valid HWND on the owning thread.
        unsafe {
            let recreate = CONTEXT.with(|c| {
                c.borrow()
                    .as_ref()
                    .is_some_and(|c| c.taskbar_created != 0 && message == c.taskbar_created)
            });
            if recreate {
                notify(hwnd, NIM_ADD);
                return 0;
            }
            match message {
                CALLBACK
                    if matches!(lparam as u32, WM_RBUTTONUP | WM_LBUTTONUP | WM_CONTEXTMENU) =>
                {
                    menu(hwnd);
                    0
                }
                WM_TIMER => {
                    notify(hwnd, NIM_MODIFY);
                    0
                }
                WM_CLOSE => {
                    DestroyWindow(hwnd);
                    0
                }
                WM_DESTROY => {
                    notify(hwnd, NIM_DELETE);
                    PostQuitMessage(0);
                    0
                }
                _ => DefWindowProcW(hwnd, message, wparam, lparam),
            }
        }
    }
}
